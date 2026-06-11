use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::{Connection, Result, core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId}};
use duckdb::vscalar::arrow::{VArrowScalar, ArrowFunctionSignature};
use arrow::array::{Array, RecordBatch, Float64Array, UInt64Array, UInt8Array};
use arrow::datatypes::DataType;
use arrow::ipc::writer::FileWriter;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Mutex, Arc};
use std::error::Error;
use std::thread;
use std::fs::File;
use base64::Engine;

// Channel payload: (Query, Filepath, ReplySender)
type Request = (String, String, mpsc::SyncSender<std::result::Result<usize, String>>);
type SenderMutex = Arc<Mutex<mpsc::SyncSender<Request>>>;

struct ArrowTilesVTab;

struct ArrowTilesBindData {
    query: String,
    filepath: String,
    tx: SenderMutex,
}

struct ArrowTilesInitData {
    done: AtomicBool,
}

impl VTab for ArrowTilesVTab {
    type InitData = ArrowTilesInitData;
    type BindData = ArrowTilesBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("rows_exported", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        
        let query = bind.get_parameter(0).to_string();
        let filepath = bind.get_parameter(1).to_string();
        
        let tx = unsafe { (*bind.get_extra_info::<SenderMutex>()).clone() };
        Ok(ArrowTilesBindData { query, filepath, tx })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(ArrowTilesInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
        } else {
            let (reply_tx, reply_rx) = mpsc::sync_channel(1);
            bind_data.tx.lock().unwrap().send((bind_data.query.clone(), bind_data.filepath.clone(), reply_tx)).unwrap();
            
            match reply_rx.recv() {
                Ok(Ok(rows)) => {
                    let mut vector = output.flat_vector(0);
                    unsafe {
                        vector.as_mut_slice::<i64>()[0] = rows as i64;
                    }
                    output.set_len(1);
                }
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => return Err("Background thread panicked or disconnected".into()),
            }
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }
}

// TODO: Migrate to CopyFunction! 
// This TableFunction + Background Worker is an architectural workaround.
// The idiomatic DuckDB solution is to implement a custom CopyFunction (e.g. COPY tbl TO 'out.pmtiles' (FORMAT 'pmtiles'))
// which natively streams DataChunks from the active transaction without breaking TEMP table scope.
fn execute_export(conn: &Connection, query: &str, filepath: &str) -> std::result::Result<usize, String> {
    let mut stmt = conn.prepare(query).map_err(|e| format!("Prepare error: {}", e))?;
    let arrow_result = stmt.query_arrow([]).map_err(|e| format!("Query arrow error: {}", e))?;
    let schema = arrow_result.get_schema();
    
    let file = std::fs::File::create(filepath).map_err(|e| format!("File create error: {}", e))?;
    
    // 1. Extract Global Schema to Base64
    let mut schema_buf = Vec::new();
    {
        // Write an empty IPC file to capture just the schema
        let mut schema_writer = arrow::ipc::writer::FileWriter::try_new(&mut schema_buf, schema.as_ref()).map_err(|e| format!("Schema write error: {}", e))?;
        schema_writer.finish().map_err(|e| format!("Schema finish error: {}", e))?;
    }
    let schema_base64 = base64::engine::general_purpose::STANDARD.encode(&schema_buf);
    let metadata_json = format!(r#"{{"format": "arrow_ipc", "compression": "none", "schema_base64": "{}"}}"#, schema_base64);

    let mut pmtiles_writer = pmtiles::PmTilesWriter::new(pmtiles::TileType::Unknown)
        .metadata(&metadata_json)
        .create(file)
        .map_err(|e| format!("PMTiles create error: {}", e))?;
    
    let mut current_tile_id: Option<u64> = None;
    let mut tile_bytes: Vec<u8> = Vec::new();
    let mut rows_exported = 0;
    let mut last_tile_id: Option<u64> = None;

    let write_options = arrow::ipc::writer::IpcWriteOptions::default();
    let data_gen = arrow::ipc::writer::IpcDataGenerator::default();

    for batch in arrow_result {
        let tile_id_col = batch.column_by_name("tile_id")
            .ok_or("Query MUST include a 'tile_id' column for PMTiles export.")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or("tile_id column must be of type UBIGINT")?;

        let mut current_run_start = 0;

        for i in 0..batch.num_rows() {
            let row_tile_id = if tile_id_col.is_valid(i) {
                Some(tile_id_col.value(i))
            } else {
                None
            };

            // Safety sort check
            if let (Some(last_id), Some(curr_id)) = (last_tile_id, row_tile_id) {
                if curr_id < last_id {
                    return Err(format!("Data is not sorted. Tile ID {} came after {}. You MUST use 'ORDER BY tile_id' in your query.", curr_id, last_id));
                }
            }
            if row_tile_id.is_some() {
                last_tile_id = row_tile_id;
            }

            if row_tile_id != current_tile_id {
                if i > 0 {
                    // Slice the run from the current batch and encode it
                    let run_length = i - current_run_start;
                    if run_length > 0 && current_tile_id.is_some() {
                        let sliced_batch = batch.slice(current_run_start, run_length);
                        // Deep copy the slice to prevent massive memory buffer bloat in IPC payload!
                        let copied_batch = arrow::compute::concat_batches(&sliced_batch.schema(), &[&sliced_batch])
                            .map_err(|e| format!("Arrow concat error: {}", e))?;
                            
                        let mut dictionary_tracker = arrow::ipc::writer::DictionaryTracker::new(false);
                        let (encoded_dictionaries, encoded_message) = data_gen
                            .encoded_batch(&copied_batch, &mut dictionary_tracker, &write_options)
                            .map_err(|e| format!("Arrow encode error: {}", e))?;

                        for dict in encoded_dictionaries {
                            arrow::ipc::writer::write_message(&mut tile_bytes, dict, &write_options).map_err(|e| format!("Arrow write error: {}", e))?;
                        }
                        arrow::ipc::writer::write_message(&mut tile_bytes, &encoded_message, &write_options).map_err(|e| format!("Arrow write error: {}", e))?;
                    }
                }
                
                // Flush the completed tile to PMTiles
                if let Some(tid) = current_tile_id {
                    if !tile_bytes.is_empty() {
                        pmtiles_writer.add_raw_tile(pmtiles::TileId::new(tid).unwrap().into(), &tile_bytes).map_err(|e| format!("PMTiles write error: {}", e))?;
                    }
                }
                
                // Reset for the new tile
                tile_bytes.clear();
                current_run_start = i;
            }

            current_tile_id = row_tile_id;
        }

        // At the end of the batch, encode any remaining rows for the current tile
        let run_length = batch.num_rows() - current_run_start;
        if run_length > 0 && current_tile_id.is_some() {
            let sliced_batch = batch.slice(current_run_start, run_length);
            // Deep copy the slice to prevent massive memory buffer bloat in IPC payload!
            let copied_batch = arrow::compute::concat_batches(&sliced_batch.schema(), &[&sliced_batch])
                .map_err(|e| format!("Arrow concat error: {}", e))?;
                
            let mut dictionary_tracker = arrow::ipc::writer::DictionaryTracker::new(false);
            let (encoded_dictionaries, encoded_message) = data_gen
                .encoded_batch(&copied_batch, &mut dictionary_tracker, &write_options)
                .map_err(|e| format!("Arrow encode error: {}", e))?;

            for dict in encoded_dictionaries {
                arrow::ipc::writer::write_message(&mut tile_bytes, dict, &write_options).map_err(|e| format!("Arrow write error: {}", e))?;
            }
            arrow::ipc::writer::write_message(&mut tile_bytes, &encoded_message, &write_options).map_err(|e| format!("Arrow write error: {}", e))?;
        }

        rows_exported += batch.num_rows();
    }

    // End of stream: flush the very last tile
    if !tile_bytes.is_empty() {
        if let Some(tid) = current_tile_id {
            pmtiles_writer.add_raw_tile(pmtiles::TileId::new(tid).unwrap().into(), &tile_bytes).map_err(|e| format!("PMTiles write error: {}", e))?;
        }
    }
    
    pmtiles_writer.finalize().map_err(|e| format!("PMTiles finalize error: {}", e))?;
    
    Ok(rows_exported)
}

struct HilbertScalar;

impl VArrowScalar for HilbertScalar {
    type State = ();

    fn invoke(_: &Self::State, input: RecordBatch) -> std::result::Result<Arc<dyn Array>, Box<dyn Error>> {
        let lon_array = input.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let lat_array = input.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let zoom_array = input.column(2).as_any().downcast_ref::<UInt8Array>().unwrap();

        let lon_iter = lon_array.iter();
        let lat_iter = lat_array.iter();
        let zoom_iter = zoom_array.iter();

        let tile_ids: Vec<Option<u64>> = lon_iter.zip(lat_iter).zip(zoom_iter).map(|((lon_opt, lat_opt), zoom_opt)| {
            match (lon_opt, lat_opt, zoom_opt) {
                (Some(lon), Some(lat), Some(zoom)) => {
                    if zoom >= 32 {
                        return None;
                    }
                    // Safe bounds check (automatically handles NaN because NaN comparisons evaluate to false)
                    if !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) {
                        return None; 
                    }

                    // Cap latitude for Web Mercator
                    let lat_clamped = lat.clamp(-85.05112878, 85.05112878);
                    let lat_rad = lat_clamped.to_radians();

                    // Calculate number of tiles across one axis at this zoom level (2^zoom)
                    let n = (1_u32 << zoom) as f64; 

                    let x = ((lon + 180.0) / 360.0 * n).floor() as u32;
                    let y = ((1.0 - lat_rad.tan().asinh() / std::f64::consts::PI) / 2.0 * n).floor() as u32;

                    // Compute true Hilbert curve index
                    let h = fast_hilbert::xy2h(x, y, zoom as u8);
                    
                    // PMTiles z-order hierarchical offset
                    let offset = ((1_u64 << (zoom * 2)) - 1) / 3;
                    
                    Some(h + offset)
                },
                _ => None // Properly yield NULL if any coordinate or zoom is missing
            }
        }).collect();

        Ok(Arc::new(UInt64Array::from(tile_ids)))
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Float64, DataType::Float64, DataType::UInt8],
            DataType::UInt64,
        )]
    }
}

#[duckdb_loadable_macros::duckdb_entrypoint_c_api(ext_name="arrowtiles")]
pub unsafe fn arrowtiles_init(conn: Connection) -> Result<(), Box<dyn Error>> {
    let (tx, rx) = mpsc::sync_channel::<Request>(1);
    let tx_mutex = Arc::new(Mutex::new(tx));

    // Register table function
    conn.register_table_function_with_extra_info::<ArrowTilesVTab, _>("arrowtiles_export", &tx_mutex)?;

    // Register scalar UDF
    conn.register_scalar_function::<HilbertScalar>("hilbert_xy")?;

    thread::spawn(move || {
        while let Ok((query, filepath, reply_tx)) = rx.recv() {
            let result = execute_export(&conn, &query, &filepath);
            let _ = reply_tx.send(result);
        }
    });

    Ok(())
}

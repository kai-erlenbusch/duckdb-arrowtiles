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

fn execute_export(conn: &Connection, query: &str, filepath: &str) -> std::result::Result<usize, String> {
    let mut stmt = conn.prepare(query).map_err(|e| format!("Prepare error: {}", e))?;
    let arrow_result = stmt.query_arrow([]).map_err(|e| format!("Query arrow error: {}", e))?;
    let schema = arrow_result.get_schema();
    
    let file = File::create(filepath).map_err(|e| format!("File creation error: {}", e))?;
    let mut writer = FileWriter::try_new(file, schema.as_ref()).map_err(|e| format!("Arrow writer init error: {}", e))?;
    
    let mut rows_exported = 0;
    for batch in arrow_result {
        writer.write(&batch).map_err(|e| format!("Arrow write error: {}", e))?;
        rows_exported += batch.num_rows();
    }
    writer.finish().map_err(|e| format!("Arrow finish error: {}", e))?;
    
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
                    let _z = zoom as u32; // will be used for fast_hilbert bounding box later
                    // Example mapping logic
                    let x = ((lon + 180.0) / 360.0 * 1000000.0) as u32;
                    let y = ((lat + 90.0) / 180.0 * 1000000.0) as u32;
                    Some((x as u64) << 32 | (y as u64))
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
    println!("🚀 ArrowTiles Extension loaded. Initializing background worker...");

    let (tx, rx) = mpsc::sync_channel::<Request>(1);
    let tx_mutex = Arc::new(Mutex::new(tx));

    // Register table function
    conn.register_table_function_with_extra_info::<ArrowTilesVTab, _>("arrowtiles_export", &tx_mutex)?;

    // Register scalar UDF
    conn.register_scalar_function::<HilbertScalar>("hilbert_xy")?;

    // TEST IF FUNCTION EXISTS
    let mut stmt = conn.prepare("SELECT hilbert_xy(1.0, 2.0, 10::UTINYINT) as val").unwrap();
    let res = stmt.query_arrow([]).unwrap();
    println!("SUCCESS! hilbert_xy exists on init connection! Rows: {:?}", res.count());

    thread::spawn(move || {
        while let Ok((query, filepath, reply_tx)) = rx.recv() {
            println!("ArrowTiles Worker: Executing inner query...");
            let result = execute_export(&conn, &query, &filepath);
            let _ = reply_tx.send(result);
        }
    });

    Ok(())
}

use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;

use ahash::AHashSet;
use arrow::array::{
    Array, Float32Array, RecordBatch, RecordBatchReader, UInt16Builder, UInt64Array, UInt64Builder, UInt8Builder,
};
use arrow::compute::{cast, filter};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use pmtiles::{Compression, PmTilesWriter, TileType};
use rayon::prelude::*;
use serde_json::json;

use pyo3::prelude::*;

#[pyfunction]
fn run_bucketer(
    py: Python,
    stream_ptr: usize,
    output_dir: String,
    grid_size: f64,
    max_zoom: u8,
    x_col_name: String,
    y_col_name: String,
) -> PyResult<()> {
    let stream_ptr = stream_ptr as *mut arrow::ffi_stream::FFI_ArrowArrayStream;
    let stream = unsafe { std::ptr::read(stream_ptr) };
    let mut reader = arrow::ffi_stream::ArrowArrayStreamReader::try_new(stream)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    let input_schema = reader.schema().clone();

    // Output schema: input schema (minus internal coords) + x_u16, y_u16, z, final_tile_id
    let mut out_fields = Vec::new();
    for f in input_schema.fields() {
        let name = f.name().as_str();
        if name != x_col_name && name != y_col_name {
            out_fields.push(f.clone());
        }
    }
    out_fields.push(Arc::new(Field::new("x_u16", DataType::UInt16, false)));
    out_fields.push(Arc::new(Field::new("y_u16", DataType::UInt16, false)));
    out_fields.push(Arc::new(Field::new("z", DataType::UInt8, false)));
    out_fields.push(Arc::new(Field::new(
        "final_tile_id",
        DataType::UInt64,
        false,
    )));

    let output_schema = Arc::new(Schema::new(out_fields));

    py.allow_threads(|| -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut writers: HashMap<u8, ArrowWriter<File>> = HashMap::new();
        let mut z_buffers: HashMap<u8, Vec<RecordBatch>> = HashMap::new();
        let mut z_buffer_rows: HashMap<u8, usize> = HashMap::new();
        let mut occupied: AHashSet<u64> = AHashSet::with_capacity(5_000_000);
        let mut row_count = 0;

        loop {
            let batch_res = Python::with_gil(|_py| reader.next());

            let batch = match batch_res {
                Some(Ok(b)) => b,
                Some(Err(e)) => return Err(e.into()),
                None => break,
            };
            
            let num_rows = batch.num_rows();
            if num_rows == 0 {
                continue;
            }
            row_count += num_rows;

            let x_col = batch
                .column_by_name(&x_col_name)
                .expect("x column missing");
            let y_col = batch
                .column_by_name(&y_col_name)
                .expect("y column missing");

            let x_f32 = cast(x_col, &DataType::Float32)?;
            let y_f32 = cast(y_col, &DataType::Float32)?;

            let x_arr = x_f32.as_any().downcast_ref::<Float32Array>().unwrap();
            let y_arr = y_f32.as_any().downcast_ref::<Float32Array>().unwrap();

            let mut z_builder = UInt8Builder::with_capacity(num_rows);
            let mut tid_builder = UInt64Builder::with_capacity(num_rows);
            let mut x_u16_builder = UInt16Builder::with_capacity(num_rows);
            let mut y_u16_builder = UInt16Builder::with_capacity(num_rows);

            for i in 0..num_rows {
                let mut x = x_arr.value(i) as f64;
                let mut y = y_arr.value(i) as f64;
                x = x.clamp(0.0, 1.0);
                y = y.clamp(0.0, 1.0);

                let mut assigned_z = max_zoom;

                for z in 0..=max_zoom {
                    let scale = (1_u64 << z) as f64;
                    let vx_z = (x * grid_size * scale) as u32;
                    let vy_z = (y * grid_size * scale) as u32;

                    let key: u64 = (z as u64) | ((vx_z as u64) << 8) | ((vy_z as u64) << 36);
                    if !occupied.contains(&key) {
                        occupied.insert(key);
                        assigned_z = z;
                        break;
                    }
                }
                z_builder.append_value(assigned_z);

                if assigned_z >= 32 {
                    tid_builder.append_null();
                    x_u16_builder.append_null();
                    y_u16_builder.append_null();
                } else {
                    let n = (1_u32 << assigned_z) as f64;
                    let max_index = (1_u32 << assigned_z).saturating_sub(1);

                    let scaled_x = x * n;
                    let scaled_y = y * n;

                    let ix = (scaled_x as u32).min(max_index);
                    let iy = (scaled_y as u32).min(max_index);

                    let local_x = scaled_x - (ix as f64);
                    let local_y = scaled_y - (iy as f64);

                    x_u16_builder.append_value((local_x * 65535.0).round().clamp(0.0, 65535.0) as u16);
                    y_u16_builder.append_value((local_y * 65535.0).round().clamp(0.0, 65535.0) as u16);

                    let h = fast_hilbert::xy2h(ix, iy, assigned_z);
                    let offset = ((1_u64 << (assigned_z * 2)) - 1) / 3;
                    tid_builder.append_value(h + offset);
                }
            }

            let z_arr = Arc::new(z_builder.finish());
            
            let mut out_columns: Vec<Arc<dyn Array>> = Vec::new();
            for f in output_schema.fields() {
                let name = f.name().as_str();
                if name == "z" {
                    out_columns.push(z_arr.clone());
                } else if name == "final_tile_id" {
                    out_columns.push(Arc::new(tid_builder.finish()));
                } else if name == "x_u16" {
                    out_columns.push(Arc::new(x_u16_builder.finish()));
                } else if name == "y_u16" {
                    out_columns.push(Arc::new(y_u16_builder.finish()));
                } else {
                    out_columns.push(batch.column_by_name(name).unwrap().clone());
                }
            }

            let out_batch = RecordBatch::try_new(output_schema.clone(), out_columns)?;
            
            // Z-level partitioning!
            for z in 0..=max_zoom {
                let mut z_mask_builder = arrow::array::BooleanBuilder::with_capacity(num_rows);
                let mut has_z = false;
                let z_arr_typed = z_arr.as_any().downcast_ref::<arrow::array::UInt8Array>().unwrap();
                for i in 0..num_rows {
                    let is_z = z_arr_typed.value(i) == z;
                    z_mask_builder.append_value(is_z);
                    if is_z { has_z = true; }
                }
                if !has_z { continue; }
                let z_mask = z_mask_builder.finish();
                
                let mut filtered_cols = Vec::new();
                for col in out_batch.columns() {
                    filtered_cols.push(filter(col, &z_mask)?);
                }
                let filtered_batch = RecordBatch::try_new(output_schema.clone(), filtered_cols)?;
                
                let buffer = z_buffers.entry(z).or_insert_with(Vec::new);
                let rows = z_buffer_rows.entry(z).or_insert(0);
                
                *rows += filtered_batch.num_rows();
                buffer.push(filtered_batch);
                
                if *rows >= 100_000 {
                    let single_batch = arrow::compute::concat_batches(&output_schema, buffer.as_slice())?;
                    let writer = writers.entry(z).or_insert_with(|| {
                        let path = format!("{}/z_{}.parquet", output_dir, z);
                        let file = File::create(path).unwrap();
                        let props = WriterProperties::builder()
                            .set_compression(parquet::basic::Compression::UNCOMPRESSED)
                            .build();
                        ArrowWriter::try_new(file, output_schema.clone(), Some(props)).unwrap()
                    });
                    writer.write(&single_batch)?;
                    buffer.clear();
                    *rows = 0;
                }
            }
        }

        for (z, buffer) in z_buffers.into_iter() {
            if !buffer.is_empty() {
                let single_batch = arrow::compute::concat_batches(&output_schema, &buffer)?;
                let writer = writers.entry(z).or_insert_with(|| {
                    let path = format!("{}/z_{}.parquet", output_dir, z);
                    let file = File::create(path).unwrap();
                    let props = WriterProperties::builder()
                        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
                        .build();
                    ArrowWriter::try_new(file, output_schema.clone(), Some(props)).unwrap()
                });
                writer.write(&single_batch)?;
            }
        }

        log::info!("Bucketed {} rows into partitions.", row_count);
        for (_, mut writer) in writers {
            writer.close()?;
        }
        Ok(())
    }).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    Ok(())
}

fn flush_chunk_buffer(
    py: Python,
    buffer: &mut Vec<(u64, Vec<RecordBatch>)>,
    rows: &mut usize,
    writer: &mut pmtiles::PmTilesStreamWriter<BufWriter<File>>,
    export_schema: &Arc<Schema>,
    global_schema_size: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if buffer.is_empty() {
        return Ok(());
    }

    let to_process = std::mem::take(buffer);
    *rows = 0;
    let schema = export_schema.clone();

    let processed_tiles = py.allow_threads(|| -> Result<Vec<(u64, Vec<u8>)>, pyo3::PyErr> {
        let processed_tiles_res: Result<Vec<(u64, Vec<u8>)>, Box<dyn std::error::Error + Send + Sync>> = to_process
            .into_par_iter()
            .map(|(tid, batches)| -> Result<(u64, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
                let mut export_batches = Vec::with_capacity(batches.len());
                for b in batches.iter() {
                    let mut cols = Vec::with_capacity(schema.fields().len());
                    for f in schema.fields() {
                        cols.push(b.column_by_name(f.name()).ok_or("Missing column")?.clone());
                    }
                    export_batches.push(RecordBatch::try_new(schema.clone(), cols)?);
                }

                let single_batch = arrow::compute::concat_batches(&schema, &export_batches)?;
                let mut sink = Vec::new();
                let mut stream_writer = StreamWriter::try_new_with_options(
                    &mut sink,
                    &schema,
                    IpcWriteOptions::default(),
                )?;
                stream_writer.write(&single_batch)?;
                stream_writer.finish()?;
                
                if sink.len() > global_schema_size {
                    let next_bytes = &sink[global_schema_size..global_schema_size + 4];
                    if next_bytes != [0xFF, 0xFF, 0xFF, 0xFF] {
                        return Err("Schema size mismatch! IPC framing shifted.".into());
                    }
                }
                
                Ok((tid, sink))
            })
            .collect();
            
        processed_tiles_res.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    })?;

    for (tid, sink) in processed_tiles {
        let coord: pmtiles::TileCoord = pmtiles::TileId::new(tid).unwrap().into();
        writer.add_tile(coord, &sink[global_schema_size..])?;
    }

    Ok(())
}

#[pyclass(unsendable)]
struct ArrowTilesPacker {
    writer: Option<pmtiles::PmTilesStreamWriter<BufWriter<File>>>,
    export_schema: Arc<Schema>,
    global_schema_size: usize,
    current_tile_id: Option<u64>,
    current_batches: Vec<RecordBatch>,
    chunk_buffer: Vec<(u64, Vec<RecordBatch>)>,
    chunk_buffer_rows: usize,
    row_count: usize,
}

#[pymethods]
impl ArrowTilesPacker {
    #[new]
    fn new(output_path: String, schema_stream_ptr: usize) -> PyResult<Self> {
        let stream_ptr = schema_stream_ptr as *mut arrow::ffi_stream::FFI_ArrowArrayStream;
        let stream = unsafe { std::ptr::read(stream_ptr) };
        let mut reader = arrow::ffi_stream::ArrowArrayStreamReader::try_new(stream)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let input_schema = reader.schema().clone();

        let mut out_fields = Vec::new();
        for f in input_schema.fields() {
            if f.name() != "final_tile_id" {
                out_fields.push(f.clone());
            }
        }
        let export_schema = Arc::new(Schema::new(out_fields));

        let mut dummy_sink = Vec::new();
        let options = IpcWriteOptions::default();
        let _stream_writer =
            StreamWriter::try_new_with_options(&mut dummy_sink, &export_schema, options)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let global_schema_size = dummy_sink.len();

        let b64_schema = BASE64_STANDARD.encode(&dummy_sink[0..global_schema_size]);
        let metadata_json = json!({ "arrow_schema": b64_schema }).to_string();

        let out_file = File::create(&output_path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let buf_writer = BufWriter::new(out_file);
        let writer = PmTilesWriter::new(TileType::Unknown)
            .tile_compression(Compression::Zstd)
            .metadata(&metadata_json)
            .create(buf_writer)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(ArrowTilesPacker {
            writer: Some(writer),
            export_schema,
            global_schema_size,
            current_tile_id: None,
            current_batches: Vec::new(),
            chunk_buffer: Vec::with_capacity(1000),
            chunk_buffer_rows: 0,
            row_count: 0,
        })
    }

    fn process_batch(&mut self, py: Python, stream_ptr: usize) -> PyResult<()> {
        let stream_ptr = stream_ptr as *mut arrow::ffi_stream::FFI_ArrowArrayStream;
        let stream = unsafe { std::ptr::read(stream_ptr) };
        let mut reader = arrow::ffi_stream::ArrowArrayStreamReader::try_new(stream)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        loop {
            let batch_res = reader.next();

            let batch = match batch_res {
                Some(Ok(b)) => b,
                Some(Err(e)) => return Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
                None => break,
            };
            
            if batch.num_rows() == 0 {
                continue;
            }

            self.row_count += batch.num_rows();

            let tile_ids_col = batch
                .column_by_name("final_tile_id")
                .expect("final_tile_id missing");
            let tile_ids = tile_ids_col
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("final_tile_id must be UInt64");

            let mut start_idx = 0;
            let mut last_tid = if tile_ids.is_null(0) {
                0
            } else {
                tile_ids.value(0)
            };

            if let Some(ctid) = self.current_tile_id {
                if ctid != last_tid {
                    let batches = std::mem::take(&mut self.current_batches);
                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    self.chunk_buffer.push((ctid, batches));
                    self.chunk_buffer_rows += rows;

                    if self.chunk_buffer_rows >= 500_000 || self.chunk_buffer.len() >= 5000 {
                        flush_chunk_buffer(
                            py,
                            &mut self.chunk_buffer,
                            &mut self.chunk_buffer_rows,
                            self.writer.as_mut().unwrap(),
                            &self.export_schema,
                            self.global_schema_size
                        ).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    }
                    self.current_tile_id = Some(last_tid);
                }
            } else {
                self.current_tile_id = Some(last_tid);
            }

            for i in 1..batch.num_rows() {
                let tid = if tile_ids.is_null(i) {
                    0
                } else {
                    tile_ids.value(i)
                };
                if tid != last_tid {
                    self.current_batches.push(batch.slice(start_idx, i - start_idx));

                    let batches = std::mem::take(&mut self.current_batches);
                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    self.chunk_buffer.push((self.current_tile_id.unwrap(), batches));
                    self.chunk_buffer_rows += rows;

                    if self.chunk_buffer_rows >= 500_000 || self.chunk_buffer.len() >= 5000 {
                        flush_chunk_buffer(
                            py,
                            &mut self.chunk_buffer,
                            &mut self.chunk_buffer_rows,
                            self.writer.as_mut().unwrap(),
                            &self.export_schema,
                            self.global_schema_size
                        ).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    }

                    self.current_tile_id = Some(tid);
                    start_idx = i;
                    last_tid = tid;
                }
            }

            if start_idx < batch.num_rows() {
                self.current_batches.push(batch.slice(start_idx, batch.num_rows() - start_idx));
            }
        }
        Ok(())
    }

    fn finalize(&mut self, py: Python) -> PyResult<()> {
        if let Some(tid) = self.current_tile_id {
            self.chunk_buffer.push((tid, std::mem::take(&mut self.current_batches)));
        }

        flush_chunk_buffer(
            py,
            &mut self.chunk_buffer,
            &mut self.chunk_buffer_rows,
            self.writer.as_mut().unwrap(),
            &self.export_schema,
            self.global_schema_size
        ).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        log::info!("Packed {} rows", self.row_count);
        
        let writer = self.writer.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("writer already finalized")
        })?;
        writer.finalize().map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }
}

#[pymodule]
fn arrowtiles_core(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_function(wrap_pyfunction!(run_bucketer, m)?)?;
    m.add_class::<ArrowTilesPacker>()?;
    Ok(())
}

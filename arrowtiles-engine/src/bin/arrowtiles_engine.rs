use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;

use ahash::AHashSet;
use arrow::array::{
    Array, Float32Array, RecordBatch, UInt16Builder, UInt64Array, UInt64Builder, UInt8Builder,
};
use arrow::compute::{cast, filter};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use pmtiles::{Compression, PmTilesWriter, TileType};
use rayon::prelude::*;
use serde_json::json;

fn run_bucketer(
    output_dir: &str,
    grid_size: f64,
    max_zoom: u8,
    x_col_name: &str,
    y_col_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let stdin = std::io::stdin();
    let reader = StreamReader::try_new(stdin.lock(), None)?;
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

    let mut writers: HashMap<u8, ArrowWriter<File>> = HashMap::new();
    let mut occupied: AHashSet<u64> = AHashSet::with_capacity(5_000_000);

    let mut row_count = 0;

    for batch_res in reader {
        let batch = batch_res?;
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }

        row_count += num_rows;

        let x_col = batch.column_by_name(x_col_name).expect(&format!("{} missing", x_col_name));
        let y_col = batch.column_by_name(y_col_name).expect(&format!("{} missing", y_col_name));

        // Graceful Float64 downcasting
        let x_cast = if x_col.data_type() == &DataType::Float64 {
            cast(x_col, &DataType::Float32)?
        } else {
            x_col.clone()
        };
        let y_cast = if y_col.data_type() == &DataType::Float64 {
            cast(y_col, &DataType::Float32)?
        } else {
            y_col.clone()
        };

        let x_arr = x_cast
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect(&format!("{} could not be downcast to Float32", x_col_name));
        let y_arr = y_cast
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect(&format!("{} could not be downcast to Float32", y_col_name));

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
            
            // Get or insert writer
            let writer = writers.entry(z).or_insert_with(|| {
                let path = format!("{}/z_{}.parquet", output_dir, z);
                let file = File::create(path).unwrap();
                let props = WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::UNCOMPRESSED)
                    .build();
                ArrowWriter::try_new(file, output_schema.clone(), Some(props)).unwrap()
            });
            writer.write(&filtered_batch)?;
        }
    }

    println!("Bucketed {} rows into partitions.", row_count);
    for (_, writer) in writers {
        writer.close()?;
    }
    Ok(())
}

fn run_packer(output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let stdin = std::io::stdin();
    let reader = StreamReader::try_new(stdin.lock(), None)?;
    let input_schema = reader.schema().clone();

    // Export schema drops final_tile_id
    let mut out_fields = Vec::new();
    for f in input_schema.fields() {
        if f.name() != "final_tile_id" {
            out_fields.push(f.clone());
        }
    }
    let export_schema = Arc::new(Schema::new(out_fields));

    // Global schema for PMTiles metadata
    let mut dummy_sink = Vec::new();
    let options = IpcWriteOptions::default();
    let _stream_writer =
        StreamWriter::try_new_with_options(&mut dummy_sink, &export_schema, options)?;
    let global_schema_size = dummy_sink.len();

    let b64_schema = BASE64_STANDARD.encode(&dummy_sink[0..global_schema_size]);
    let metadata_json = json!({ "arrow_schema": b64_schema }).to_string();

    let out_file = File::create(output_path)?;
    let buf_writer = BufWriter::new(out_file);
    let mut writer = PmTilesWriter::new(TileType::Unknown)
        .tile_compression(Compression::Zstd)
        .metadata(&metadata_json)
        .create(buf_writer)?;

    let mut current_tile_id: Option<u64> = None;
    let mut current_batches: Vec<RecordBatch> = Vec::new();
    let mut chunk_buffer: Vec<(u64, Vec<RecordBatch>)> = Vec::with_capacity(1000);
    let mut chunk_buffer_rows = 0;

    let flush_chunk_buffer = |buffer: &mut Vec<(u64, Vec<RecordBatch>)>,
                              rows: &mut usize,
                              writer: &mut pmtiles::PmTilesStreamWriter<BufWriter<File>>|
     -> Result<(), Box<dyn std::error::Error>> {
        if buffer.is_empty() {
            return Ok(());
        }

        let to_process = std::mem::take(buffer);
        *rows = 0;
        let schema = export_schema.clone();

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

                let mut sink = Vec::new();
                let mut stream_writer = StreamWriter::try_new_with_options(
                    &mut sink,
                    &schema,
                    IpcWriteOptions::default(),
                )?;
                for b in export_batches.iter() {
                    stream_writer.write(b)?;
                }
                stream_writer.finish()?;
                
                // Dynamic Schema Stripping Safety Check!
                if sink.len() > global_schema_size {
                    let next_bytes = &sink[global_schema_size..global_schema_size + 4];
                    if next_bytes != [0xFF, 0xFF, 0xFF, 0xFF] {
                        return Err("Schema size mismatch! IPC framing shifted.".into());
                    }
                }
                
                Ok((tid, sink))
            })
            .collect();
            
        let processed_tiles = processed_tiles_res.map_err(|e| e as Box<dyn std::error::Error>)?;

        for (tid, sink) in processed_tiles {
            let coord: pmtiles::TileCoord = pmtiles::TileId::new(tid).unwrap().into();
            writer.add_tile(coord, &sink[global_schema_size..])?;
        }

        Ok(())
    };

    let mut row_count = 0;

    for batch_res in reader {
        let batch = batch_res?;
        if batch.num_rows() == 0 {
            continue;
        }

        row_count += batch.num_rows();

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

        if let Some(ctid) = current_tile_id {
            if ctid != last_tid {
                let batches = std::mem::take(&mut current_batches);
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                chunk_buffer.push((ctid, batches));
                chunk_buffer_rows += rows;

                if chunk_buffer_rows >= 500_000 || chunk_buffer.len() >= 5000 {
                    flush_chunk_buffer(&mut chunk_buffer, &mut chunk_buffer_rows, &mut writer)?;
                }
                current_tile_id = Some(last_tid);
            }
        } else {
            current_tile_id = Some(last_tid);
        }

        for i in 1..batch.num_rows() {
            let tid = if tile_ids.is_null(i) {
                0
            } else {
                tile_ids.value(i)
            };
            if tid != last_tid {
                current_batches.push(batch.slice(start_idx, i - start_idx));

                let batches = std::mem::take(&mut current_batches);
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                chunk_buffer.push((current_tile_id.unwrap(), batches));
                chunk_buffer_rows += rows;

                if chunk_buffer_rows >= 500_000 || chunk_buffer.len() >= 5000 {
                    flush_chunk_buffer(&mut chunk_buffer, &mut chunk_buffer_rows, &mut writer)?;
                }

                current_tile_id = Some(tid);
                start_idx = i;
                last_tid = tid;
            }
        }

        if start_idx < batch.num_rows() {
            current_batches.push(batch.slice(start_idx, batch.num_rows() - start_idx));
        }
    }

    if let Some(tid) = current_tile_id {
        chunk_buffer.push((tid, std::mem::take(&mut current_batches)));
    }

    flush_chunk_buffer(&mut chunk_buffer, &mut chunk_buffer_rows, &mut writer)?;

    println!("Packed {} rows", row_count);
    writer.finalize()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: arrowtiles_engine <mode> [options]");
        eprintln!("Modes:");
        eprintln!("  --bucketer <output_dir> <grid_size> <max_zoom>");
        eprintln!("  --packer <output.arrowtiles>");
        std::process::exit(1);
    }

    let mode = &args[1];
    match mode.as_str() {
        "--bucketer" => {
            if args.len() < 5 {
                eprintln!(
                    "Usage: arrowtiles_engine --bucketer <output_dir> <grid_size> <max_zoom> [--x-col <col>] [--y-col <col>]"
                );
                std::process::exit(1);
            }
            let output_dir = &args[2];
            let grid_size: f64 = args[3].parse()?;
            let max_zoom: u8 = args[4].parse()?;
            
            let mut x_col_name = "x_norm".to_string();
            let mut y_col_name = "y_norm".to_string();
            
            let mut i = 5;
            while i < args.len() {
                if args[i] == "--x-col" && i + 1 < args.len() {
                    x_col_name = args[i+1].clone();
                    i += 2;
                } else if args[i] == "--y-col" && i + 1 < args.len() {
                    y_col_name = args[i+1].clone();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            
            run_bucketer(output_dir, grid_size, max_zoom, &x_col_name, &y_col_name)?;
        }
        "--packer" => {
            if args.len() < 3 {
                eprintln!("Usage: arrowtiles_engine --packer <output.arrowtiles>");
                std::process::exit(1);
            }
            let output_path = &args[2];
            run_packer(output_path)?;
        }
        _ => {
            eprintln!("Unknown mode: {}", mode);
            std::process::exit(1);
        }
    }

    Ok(())
}

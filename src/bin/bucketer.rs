use ahash::AHashSet;
use std::env;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{Array, Float32Array, UInt64Builder, UInt8Builder, UInt16Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 5 {
        eprintln!("Usage: arrowtiles_bucketer <input.parquet> <output.parquet> <grid_size> <max_zoom>");
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_path = &args[2];
    let grid_size: f64 = args[3].parse()?;
    let max_zoom: u8 = args[4].parse()?;

    // Open input Parquet
    let file = File::open(input_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let input_schema = builder.schema().clone();
    let reader = builder.with_batch_size(500_000).build()?;

    // Create output schema (input schema + z + final_tile_id)
    let fields = input_schema.fields().iter().cloned().collect::<Vec<_>>();
    
    // We expect input to have at least abs_m, bp_rp
    let mut out_fields = Vec::new();
    for f in fields.iter() {
        if f.name() == "abs_m" || f.name() == "bp_rp" {
            out_fields.push(f.clone());
        }
    }
    out_fields.push(Arc::new(Field::new("x_u16", DataType::UInt16, false)));
    out_fields.push(Arc::new(Field::new("y_u16", DataType::UInt16, false)));
    out_fields.push(Arc::new(Field::new("z", DataType::UInt8, false)));
    out_fields.push(Arc::new(Field::new("final_tile_id", DataType::UInt64, false)));
    
    let output_schema = Arc::new(Schema::new(out_fields));

    // Open output Parquet
    let out_file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .build();
    let mut writer = ArrowWriter::try_new(out_file, output_schema.clone(), Some(props))?;

    let mut occupied: AHashSet<(u8, u32, u32)> = AHashSet::with_capacity(5_000_000);

    for batch_res in reader {
        let batch = batch_res?;
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }

        // Get arrays
        let x_col = batch.column_by_name("x_norm").expect("x_norm missing");
        let y_col = batch.column_by_name("y_norm").expect("y_norm missing");
        
        let x_arr = x_col.as_any().downcast_ref::<Float32Array>().expect("x_norm must be Float32");
        let y_arr = y_col.as_any().downcast_ref::<Float32Array>().expect("y_norm must be Float32");

        let mut z_builder = UInt8Builder::with_capacity(num_rows);
        let mut tid_builder = UInt64Builder::with_capacity(num_rows);
        let mut x_u16_builder = UInt16Builder::with_capacity(num_rows);
        let mut y_u16_builder = UInt16Builder::with_capacity(num_rows);

        for i in 0..num_rows {
            let mut x = x_arr.value(i) as f64;
            let mut y = y_arr.value(i) as f64;
            
            if x < 0.0 { x = 0.0; }
            if x > 1.0 { x = 1.0; }
            if y < 0.0 { y = 0.0; }
            if y > 1.0 { y = 1.0; }
            
            let mut assigned_z = max_zoom;

            for z in 0..=max_zoom {
                let scale = (1_u64 << z) as f64;
                let vx_z = (x * grid_size * scale) as u32;
                let vy_z = (y * grid_size * scale) as u32;

                let key = (z, vx_z, vy_z);
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

        // Build output batch
        let mut out_columns: Vec<Arc<dyn Array>> = Vec::new();
        for f in output_schema.fields() {
            if f.name() == "z" {
                out_columns.push(Arc::new(z_builder.finish()));
            } else if f.name() == "final_tile_id" {
                out_columns.push(Arc::new(tid_builder.finish()));
            } else if f.name() == "x_u16" {
                out_columns.push(Arc::new(x_u16_builder.finish()));
            } else if f.name() == "y_u16" {
                out_columns.push(Arc::new(y_u16_builder.finish()));
            } else {
                out_columns.push(batch.column_by_name(f.name()).unwrap().clone());
            }
        }

        let out_batch = RecordBatch::try_new(output_schema.clone(), out_columns)?;
        writer.write(&out_batch)?;
    }

    writer.close()?;
    Ok(())
}

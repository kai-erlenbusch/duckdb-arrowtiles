use std::env;
use std::fs::File;
use std::sync::Arc;
use std::io::BufWriter;

use arrow::array::{Array, UInt64Array};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::{StreamWriter, IpcWriteOptions};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pmtiles::{Compression, PmTilesWriter, TileType};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: arrowtiles_packer <input_sorted.parquet> <output.pmtiles>");
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_path = &args[2];

    // Open input Parquet
    let file = File::open(input_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let input_schema = builder.schema().clone();
    let reader = builder.with_batch_size(500_000).build()?;

    // Create export schema (drop final_tile_id)
    let mut out_fields = Vec::new();
    for f in input_schema.fields() {
        if f.name() != "final_tile_id" {
            out_fields.push(f.clone());
        }
    }
    let export_schema = Arc::new(Schema::new(out_fields));

    // Open PMTiles writer
    let out_file = File::create(output_path)?;
    let buf_writer = BufWriter::new(out_file);
    let mut writer = PmTilesWriter::new(TileType::Unknown)
        .tile_compression(Compression::Zstd)
        .create(buf_writer)?;

    let mut current_tile_id: Option<u64> = None;
    let mut current_batches: Vec<RecordBatch> = Vec::new();

    let flush_tile = |tid: u64, batches: &mut Vec<RecordBatch>, pmtiles_writer: &mut pmtiles::PmTilesStreamWriter<BufWriter<File>>| -> Result<(), Box<dyn std::error::Error>> {
        if batches.is_empty() {
            return Ok(());
        }

        // Drop final_tile_id column from all batches
        let mut export_batches = Vec::with_capacity(batches.len());
        for b in batches.iter() {
            let mut cols = Vec::new();
            for f in export_schema.fields() {
                cols.push(b.column_by_name(f.name()).unwrap().clone());
            }
            export_batches.push(RecordBatch::try_new(export_schema.clone(), cols)?);
        }

        // Write to Arrow IPC Stream
        let mut sink = Vec::new();
        {
            let options = IpcWriteOptions::default();
            let mut stream_writer = StreamWriter::try_new_with_options(&mut sink, &export_schema, options)?;
            for b in export_batches {
                stream_writer.write(&b)?;
            }
            stream_writer.finish()?;
        }

        // Add to PMTiles
        let coord: pmtiles::TileCoord = pmtiles::TileId::new(tid).unwrap().into();
        pmtiles_writer.add_tile(coord, &sink)?;
        batches.clear();
        Ok(())
    };

    for batch_res in reader {
        let batch = batch_res?;
        if batch.num_rows() == 0 {
            continue;
        }

        let tile_ids_col = batch.column_by_name("final_tile_id").expect("final_tile_id missing");
        let tile_ids = tile_ids_col.as_any().downcast_ref::<UInt64Array>().expect("final_tile_id must be UInt64");

        let mut start_idx = 0;
        let mut last_tid = if tile_ids.is_null(0) { 0 } else { tile_ids.value(0) };

        for i in 1..batch.num_rows() {
            let tid = if tile_ids.is_null(i) { 0 } else { tile_ids.value(i) };
            if tid != last_tid {
                // Split boundary found
                if current_tile_id.is_none() {
                    current_tile_id = Some(last_tid);
                }

                if Some(last_tid) != current_tile_id {
                    flush_tile(current_tile_id.unwrap(), &mut current_batches, &mut writer)?;
                    current_tile_id = Some(last_tid);
                }

                current_batches.push(batch.slice(start_idx, i - start_idx));
                start_idx = i;
                last_tid = tid;
            }
        }

        // Handle remainder
        if current_tile_id.is_none() {
            current_tile_id = Some(last_tid);
        }
        if Some(last_tid) != current_tile_id {
            flush_tile(current_tile_id.unwrap(), &mut current_batches, &mut writer)?;
            current_tile_id = Some(last_tid);
        }
        if start_idx < batch.num_rows() {
            current_batches.push(batch.slice(start_idx, batch.num_rows() - start_idx));
        }
    }

    if let Some(tid) = current_tile_id {
        flush_tile(tid, &mut current_batches, &mut writer)?;
    }

    writer.finalize()?;
    Ok(())
}

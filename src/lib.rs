use duckdb::{Connection, Result, core::LogicalTypeId};
use duckdb::vscalar::arrow::{VArrowScalar, ArrowFunctionSignature};
use arrow::array::{Array, RecordBatch, Float64Array, UInt8Array, UInt64Builder};
use arrow::datatypes::DataType;
use std::sync::Arc;
use std::error::Error;

struct HilbertScalar;

impl VArrowScalar for HilbertScalar {
    type State = ();

    fn invoke(_: &Self::State, input: RecordBatch) -> std::result::Result<Arc<dyn Array>, Box<dyn Error>> {
        let lon_array = input.column(0).as_any().downcast_ref::<Float64Array>()
            .ok_or("Failed to downcast longitude column to Float64")?;
        let lat_array = input.column(1).as_any().downcast_ref::<Float64Array>()
            .ok_or("Failed to downcast latitude column to Float64")?;
        let zoom_array = input.column(2).as_any().downcast_ref::<UInt8Array>()
            .ok_or("Failed to downcast zoom column to UInt8")?;

        let mut builder = UInt64Builder::with_capacity(input.num_rows());

        if lon_array.null_count() == 0 && lat_array.null_count() == 0 && zoom_array.null_count() == 0 && input.num_rows() > 0 {
            let lon_vals = lon_array.values();
            let lat_vals = lat_array.values();
            let zoom = zoom_array.value(0); // Extract constant zoom

            let n = (1_u32 << zoom) as f64;
            let max_index = (n as u32) - 1;
            let offset = ((1_u64 << (zoom * 2)) - 1) / 3;

            for (&lon, &lat) in lon_vals.iter().zip(lat_vals.iter()) {
                if zoom >= 32 || !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) {
                    builder.append_null();
                    continue;
                }
                
                let lat_clamped = lat.clamp(-85.05112878, 85.05112878);
                let lat_rad = lat_clamped.to_radians();
                let x = (((lon + 180.0) / 360.0 * n).floor() as u32).min(max_index);
                let y = (((1.0 - lat_rad.tan().asinh() / std::f64::consts::PI) / 2.0 * n).floor() as u32).min(max_index);

                let h = fast_hilbert::xy2h(x, y, zoom as u8);
                builder.append_value(h + offset);
            }
        } else {
            // Fallback for nulls
            let lon_iter = lon_array.iter();
            let lat_iter = lat_array.iter();
            let zoom_iter = zoom_array.iter();

            for ((lon_opt, lat_opt), zoom_opt) in lon_iter.zip(lat_iter).zip(zoom_iter) {
                match (lon_opt, lat_opt, zoom_opt) {
                    (Some(lon), Some(lat), Some(zoom)) => {
                        if zoom >= 32 || !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) {
                            builder.append_null();
                            continue;
                        }

                        let lat_clamped = lat.clamp(-85.05112878, 85.05112878);
                        let lat_rad = lat_clamped.to_radians();
                        let n = (1_u32 << zoom) as f64;
                        let max_index = (n as u32) - 1;
                        let x = (((lon + 180.0) / 360.0 * n).floor() as u32).min(max_index);
                        let y = (((1.0 - lat_rad.tan().asinh() / std::f64::consts::PI) / 2.0 * n).floor() as u32).min(max_index);

                        let h = fast_hilbert::xy2h(x, y, zoom as u8);
                        let offset = ((1_u64 << (zoom * 2)) - 1) / 3;
                        builder.append_value(h + offset);
                    },
                    _ => builder.append_null(),
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Float64, DataType::Float64, DataType::UInt8],
            DataType::UInt64,
        )]
    }
}

struct HilbertNormalizedScalar;

impl VArrowScalar for HilbertNormalizedScalar {
    type State = ();

    fn invoke(_: &Self::State, input: RecordBatch) -> std::result::Result<Arc<dyn Array>, Box<dyn Error>> {
        let x_array = input.column(0).as_any().downcast_ref::<Float64Array>()
            .ok_or("Failed to downcast x column to Float64")?;
        let y_array = input.column(1).as_any().downcast_ref::<Float64Array>()
            .ok_or("Failed to downcast y column to Float64")?;
        let zoom_array = input.column(2).as_any().downcast_ref::<UInt8Array>()
            .ok_or("Failed to downcast zoom column to UInt8")?;

        let mut builder = UInt64Builder::with_capacity(input.num_rows());

        if x_array.null_count() == 0 && y_array.null_count() == 0 && zoom_array.null_count() == 0 && input.num_rows() > 0 {
            let x_vals = x_array.values();
            let y_vals = y_array.values();
            let zoom = zoom_array.value(0); // Extract constant zoom

            let n = (1_u32 << zoom) as f64;
            let max_index = (n as u32) - 1;
            let offset = ((1_u64 << (zoom * 2)) - 1) / 3;

            for (&x, &y) in x_vals.iter().zip(y_vals.iter()) {
                if zoom >= 32 {
                    builder.append_null();
                    continue;
                }
                
                let ix = ((x.clamp(0.0, 1.0) * n).floor() as u32).min(max_index);
                let iy = ((y.clamp(0.0, 1.0) * n).floor() as u32).min(max_index);

                let h = fast_hilbert::xy2h(ix, iy, zoom as u8);
                builder.append_value(h + offset);
            }
        } else {
            let x_iter = x_array.iter();
            let y_iter = y_array.iter();
            let zoom_iter = zoom_array.iter();

            for ((x_opt, y_opt), zoom_opt) in x_iter.zip(y_iter).zip(zoom_iter) {
                match (x_opt, y_opt, zoom_opt) {
                    (Some(x), Some(y), Some(zoom)) => {
                        if zoom >= 32 {
                            builder.append_null();
                            continue;
                        }
                        let n = (1_u32 << zoom) as f64;
                        let max_index = (n as u32) - 1;

                        let ix = ((x.clamp(0.0, 1.0) * n).floor() as u32).min(max_index);
                        let iy = ((y.clamp(0.0, 1.0) * n).floor() as u32).min(max_index);

                        let h = fast_hilbert::xy2h(ix, iy, zoom as u8);
                        let offset = ((1_u64 << (zoom * 2)) - 1) / 3;
                        builder.append_value(h + offset);
                    },
                    _ => builder.append_null(),
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Float64, DataType::Float64, DataType::UInt8],
            DataType::UInt64,
        )]
    }
}

// Register scalar UDFs
#[duckdb_loadable_macros::duckdb_entrypoint_c_api(ext_name="arrowtiles")]
pub unsafe fn arrowtiles_init(conn: Connection) -> Result<(), Box<dyn Error>> {
    conn.register_scalar_function::<HilbertScalar>("hilbert_xy")?;
    conn.register_scalar_function::<HilbertNormalizedScalar>("hilbert_normalized")?;

    Ok(())
}

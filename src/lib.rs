use duckdb::Connection;
use std::error::Error;

/// The main entrypoint for the DuckDB extension.
/// DuckDB will call this function when the user runs `LOAD arrowtiles;`
#[duckdb_loadable_macros::duckdb_entrypoint_c_api(ext_name="arrowtiles")]
pub unsafe fn arrowtiles_init(_conn: Connection) -> Result<(), Box<dyn Error>> {
    println!("🚀 ArrowTiles Extension successfully loaded into DuckDB!");

    // TODO: Phase 1 & 2 - Register a custom Table Function or COPY handler
    // e.g., conn.execute("CREATE FUNCTION ...", [])?;

    Ok(())
}

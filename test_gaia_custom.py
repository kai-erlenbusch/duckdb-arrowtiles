import time
from arrowtiles import ArrowTilesBuilder

def run_custom_gaia():
    # Example showing how a data scientist would use the native PyO3 ArrowTiles Python package
    # to define custom variables directly from an SQL query, without using CLI flags.

    print("🚀 Initializing Native PyO3 ArrowTiles Engine...")
    
    # We have added automatic thread guardrails (leaves 2 cores free for the OS)
    # and a safer default memory limit to prevent system freezing.
    builder = ArrowTilesBuilder(memory_limit="40GB", temp_dir="./duckdb_temp")
    
    # Path to the actual Gaia parquet file
    # (Adjust this path if your gaia.parquet is located elsewhere)
    input_parquet = r"D:\exploratory\duckdb-extension\deepgraph-arrowtiles-sandbox\s3_cache\batch_*.parquet"
    output_arrowtiles = "gaia_full.arrowtiles"

    # Define the exact variables we want to extract and map from the dataset.
    # We apply the Hammer projection for the spatial coordinates (x_norm, y_norm)
    # and explicitly pull bp_rp (color) and radial_velocity (kinematics) as future auxiliary layers.
    custom_query = f"""
        WITH raw_data AS (
            SELECT 
                ra, dec, magnitude, bv, parallax, pmra, pmdec, radial_velocity, teff_gspphot,
                RADIANS(ra) AS ra_rad,
                RADIANS(dec) AS dec_rad,
                RADIANS(192.85948) AS a_g,
                RADIANS(27.12825) AS d_g,
                RADIANS(122.93192) AS l_ncp
            FROM read_parquet('{input_parquet}')
        ),
        galactic AS (
            SELECT 
                *,
                ASIN(SIN(d_g)*SIN(dec_rad) + COS(d_g)*COS(dec_rad)*COS(ra_rad - a_g)) AS b_rad,
                l_ncp - ATAN2(
                    COS(dec_rad)*SIN(ra_rad - a_g), 
                    COS(d_g)*SIN(dec_rad) - SIN(d_g)*COS(dec_rad)*COS(ra_rad - a_g)
                ) AS l_rad_raw
            FROM raw_data
        ),
        wrapped AS (
            SELECT *, ((l_rad_raw + 5*PI()) % (2*PI())) - PI() AS l_rad FROM galactic
            WHERE ra IS NOT NULL AND dec IS NOT NULL
        )
        SELECT 
            CAST(( ( -2 * sqrt(2) * cos(b_rad) * sin(l_rad / 2) ) / sqrt(1 + cos(b_rad) * cos(l_rad / 2)) + 2.8284271247461903 ) / 5.6568542494923806 AS FLOAT) AS x_norm,
            1.0 - CAST(( ((sqrt(2) * sin(b_rad)) / sqrt(1 + cos(b_rad) * cos(l_rad / 2))) + 1.4142135623730951 ) / 2.8284271247461903 AS FLOAT) AS y_norm,
            CAST(magnitude AS FLOAT) AS abs_m,
            
            -- Auxiliary Layer Variables (Phase 2 preview)
            CAST(bv AS FLOAT) AS bp_rp,
            CAST(radial_velocity AS FLOAT) AS radial_velocity,
            CAST(teff_gspphot AS FLOAT) AS teff_gspphot,
            CAST(parallax AS FLOAT) AS parallax,
            CAST(pmra AS FLOAT) AS pmra,
            CAST(pmdec AS FLOAT) AS pmdec
        FROM wrapped
    """
    
    print(f"📦 Starting Build Process for {output_arrowtiles}...")
    start_time = time.time()
    
    # Execute the build natively!
    builder.build(
        input_query=custom_query,
        output_path=output_arrowtiles,
        sort_col="abs_m",  # Used for LOD Quadtree sorting
        x_col="x_norm",
        y_col="y_norm",
        max_capacity=100000,
        max_zoom=14,
        resume=False # Set to True if Pass 1 succeeds but Pass 2 fails
    )

    elapsed = time.time() - start_time
    print(f"✅ Build completed successfully in {elapsed:.2f} seconds!")

if __name__ == "__main__":
    run_custom_gaia()

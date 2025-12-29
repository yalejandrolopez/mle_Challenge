"""
Spatial join pipeline for enriching DVF data with geographic codes.

This pipeline adds geographic identifiers to DVF transactions:
- IRIS codes (neighborhood level) via spatial join
- Enriches department/commune codes if missing
- Optional: postcode verification via spatial join

Two approaches:
1. If coordinates exist: Create point geometries and spatial join
2. If no coordinates: Use INSEE codes (fallback, IRIS optional)
"""

from __future__ import annotations

import sys
import polars as pl

# Add project root to path to import config


# ----------------------------
# Config
# ----------------------------

# Check if we have coordinates in the data
COORDINATE_COLUMNS = ["Longitude", "Latitude"]  # Update if different in DVF


# ----------------------------
# Spatial join helpers
# ----------------------------

def has_coordinates(df: pl.DataFrame) -> bool:
    """Check if the dataframe has coordinate columns."""
    return all(col in df.columns for col in COORDINATE_COLUMNS)


def add_iris_codes_spatial(df: pl.DataFrame, iris_path: Path) -> pl.DataFrame:
    """
    Add IRIS codes via spatial join (requires geopandas).

    This is a placeholder - actual implementation requires:
    1. geopandas for spatial operations
    2. Converting DVF points to GeoDataFrame
    logger.warning("Spatial join with IRIS requires geopandas")
    logger.info("Install with: pip install geopandas")
    logger.info("For now, IRIS codes will be skipped")
        df: DVF dataframe with coordinates
        iris_path: Path to IRIS boundaries (GeoPackage)

    Returns:
        DVF dataframe with CODE_IRIS column added
    """
    print("\n⚠️  Spatial join with IRIS requires geopandas")
    print("   Install with: pip install geopandas")
    print("   For now, IRIS codes will be skipped.\n")

    # TODO: Implement spatial join
    # import geopandas as gpd
    # from shapely.geometry import Point
    #
    # # Create GeoDataFrame from DVF points
    # geometry = [Point(xy) for xy in zip(df['Longitude'], df['Latitude'])]
    # gdf_dvf = gpd.GeoDataFrame(df.to_pandas(), geometry=geometry, crs="EPSG:4326")
    #
    # # Load IRIS boundaries
    # gdf_iris = gpd.read_file(iris_path)
    #
    # # Spatial join
    # gdf_joined = gpd.sjoin(gdf_dvf, gdf_iris[['CODE_IRIS', 'geometry']], how='left')
    #
    # # Convert back to Polars
    # return pl.from_pandas(gdf_joined.drop(columns=['geometry']))

    return df


def add_iris_codes_fallback(df: pl.DataFrame) -> pl.DataFrame:
    """
    logger.warning("IRIS codes cannot be added without coordinates")
    logger.info("IRIS-level aggregation will be skipped")
    logger.info("This is acceptable for the demo")
    Args:
        df: DVF dataframe

    Returns:
        Original dataframe (unchanged)
    """
    print("\n⚠️  IRIS codes cannot be added without coordinates")
    print("   IRIS-level aggregation will be skipped.")
    print("   This is acceptable for the demo.\n")
    return df


# ----------------------------
# Main pipeline
# ----------------------------

def enrich_with_geometries(
    input_path: Path | None = None,
    output_path: Path | None = None,
    iris_path: Path | None = None,
) -> pl.DataFrame:
    """
    Enrich DVF data with geographic codes.

    Args:
        input_path: Path to cleaned DVF parquet (default: from config)
        output_path: Path to save enriched data (default: from config)
        iris_path: Path to IRIS boundaries (default: from config)

    Returns:
        Enriched DVF dataframe
    """
    try:
        # Use centralized paths if not provided
        if input_path is None:
            input_path = get_dvf_clean_path()
        if output_path is None:
            output_path = get_dvf_with_geometries_path()
        if iris_path is None:
            iris_path = get_iris_boundaries_path()

        logger.info("=" * 70)
        logger.info("SPATIAL JOIN PIPELINE")
        logger.info("=" * 70)
        logger.info(f"\nInput: {input_path}")

        # Load cleaned data
        logger.info("Loading cleaned DVF data")
        df = pl.read_parquet(input_path)
        logger.info(f"Loaded {len(df):,} transactions")

        # Check for coordinates
        if has_coordinates(df):
            logger.info("✓ Coordinates found in data")
            logger.info("Attempting spatial join with IRIS boundaries")

            if iris_path.exists():
                df = add_iris_codes_spatial(df, iris_path)
            else:
                logger.warning(f"IRIS boundaries not found at {iris_path}")
                logger.info("Download from: https://geoservices.ign.fr/contoursiris")
        else:
            logger.info("No coordinates found in data")
            logger.info("Using INSEE codes for region/department/commune")
            df = add_iris_codes_fallback(df)

        # Save enriched data
        logger.info(f"Saving enriched data to {output_path}")
        df.write_parquet(output_path)
        logger.info(f"✓ Saved {len(df):,} transactions")

        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("ENRICHMENT SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Total transactions: {len(df):,}")

        if "CODE_IRIS" in df.columns:
            iris_count = df.filter(pl.col("CODE_IRIS").is_not_null()).height
            logger.info(f"Transactions with IRIS: {iris_count:,} ({iris_count/len(df)*100:.1f}%)")
        else:
            logger.info("IRIS codes: Not added (requires spatial join)")

        logger.info("\n✅ Spatial join complete!")
        return df

    except Exception as e:
        logger.error(f"Error in spatial join pipeline: {e}", exc_info=True)
        raise


# ----------------------------
# ----------------------------
# Main execution
# ----------------------------

if __name__ == "__main__":
    try:
        # Ensure directories exist
        ensure_data_directories()

        # Run enrichment
        df_enriched = enrich_with_geometries()

        logger.info("\n" + "=" * 70)
        logger.info("NEXT STEPS")
        logger.info("=" * 70)
        logger.info("\n1. Run aggregate.py again to include IRIS-level aggregation")
        logger.info("   (if IRIS codes were successfully added)")
        logger.info("\n2. Proceed to build_tiles.py for vector tile generation")
        logger.info("\n" + "=" * 70)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)

"""
Real estate price aggregation pipeline

Aggregates DVF transaction data to multiple geographic levels:
- Country (national)
- Region
- Department
- Commune (city)
- Postcode
- IRIS (neighborhood) - when available

For each level + property type, computes:
- n_sales: number of transactions
- median_price_m2: median price per square meter
- p25_price_m2: 25th percentile
- p75_price_m2: 75th percentile
- last_tx_date: most recent transaction date

Only keeps areas with >= MIN_SALES transactions for stability.
"""

from __future__ import annotations

import sys
import logging
from pathlib import Path
import polars as pl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Add project root to path to import config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.paths import (
    get_dvf_clean_path,
    get_mart_path,
    ensure_data_directories,
    MART_DATA_DIR,
)


# ----------------------------
# Config
# ----------------------------

# Minimum sales thresholds by aggregation level
# Higher thresholds for broader geographies to ensure reliable estimates
MIN_SALES_BY_LEVEL = {
    'country': 100,      # National level - need solid estimate
    'region': 100,       # Regional level - large areas, need reliability
    'department': 50,    # Department level - medium threshold
    'commune': 10,       # City level - keep lower for coverage
    'postcode': 10,      # Postcode level - keep lower for coverage
    'iris': 10,          # Neighborhood level - keep lower for coverage
}

# Default fallback
MIN_SALES_DEFAULT = 10


# ----------------------------
# Core aggregation logic
# ----------------------------

def aggregate_price(
    df: pl.DataFrame,
    group_cols: list[str],
    min_sales: int = MIN_SALES_DEFAULT,
) -> pl.DataFrame:
    """
    Aggregate €/m² by area + property type.

    Args:
        df: Clean DVF dataframe
        group_cols: List of columns to group by (e.g., ['Code departement'])
        min_sales: Minimum number of sales to keep an area (default: 10)

    Returns:
        Aggregated dataframe with metrics per area + property type
    """
    return (
        df.group_by(group_cols + ["Type local"])
        .agg(
            n_sales=pl.len(),
            median_price_m2=pl.col("price_m2").median(),
            p25_price_m2=pl.col("price_m2").quantile(0.25),
            p75_price_m2=pl.col("price_m2").quantile(0.75),
            last_tx_date=pl.col("Date mutation").max(),
        )
        .filter(pl.col("n_sales") >= min_sales)
        .sort(group_cols + ["Type local"])
    )


def add_region_code(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add region code using official department-to-region mapping.
    Automatically loads the mapping from the official geometry file.
    France has 13 administrative regions (post-2016 reform).
    """
    try:
        import geopandas as gpd

        # Load official department → region mapping from geometry file
        gpkg_path = Path(__file__).parent.parent / 'data' / 'raw' / 'ADE_4-0_GPKG_LAMB93_FXX-ED2025-12-05.gpkg'
        dept_gdf = gpd.read_file(gpkg_path, layer='DEPARTEMENT')

        # Extract mapping from 'code_insee_de_la_region' column
        if 'code_insee_de_la_region' in dept_gdf.columns:
            dept_to_region = dict(zip(
                dept_gdf['code_insee'].astype(str),
                dept_gdf['code_insee_de_la_region'].astype(str)
            ))

            # Apply mapping
            return df.with_columns(
                pl.col("Code departement")
                .cast(str)
                .replace(dept_to_region, default=None)
                .alias("Code region")
            )
        else:
            logger.warning("'code_insee_de_la_region' column not found")
            logger.warning("Skipping region code assignment")
            return df.with_columns(pl.lit(None).alias("Code region"))

    except Exception as e:
        logger.warning(f"Could not load department-region mapping: {e}")
        logger.warning("Skipping region code assignment")
        return df.with_columns(pl.lit(None).alias("Code region"))


# ----------------------------
# Multi-level aggregation
# ----------------------------

def aggregate_all_levels(
    input_path: Path | None = None,
    output_dir: Path | None = None,
) -> dict[str, pl.DataFrame]:
    """
    Generate all aggregation levels from cleaned DVF data.

    Args:
        input_path: Path to cleaned DVF parquet file (default: from config)
        output_dir: Directory to save mart files (default: from config)

    Returns:
        Dictionary mapping level name to aggregated dataframe
    """
    try:
        # Use centralized paths if not provided
        if input_path is None:
            input_path = get_dvf_clean_path()
        if output_dir is None:
            output_dir = MART_DATA_DIR

        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        # Load cleaned data
        logger.info(f"Loading cleaned DVF data from {input_path}")
        df = pl.read_parquet(input_path)
        logger.info(f"Loaded {len(df):,} transactions")

        # Add region code for regional aggregation
        df = add_region_code(df)

        results = {}

        # 1. Country level
        logger.info("\nStep 1: Aggregating at COUNTRY level")
        country_agg = aggregate_price(df, [], min_sales=MIN_SALES_BY_LEVEL['country'])
        output_path = get_mart_path("country")
        country_agg.write_parquet(output_path)
        results["country"] = country_agg
        logger.info(f"✓ Saved {len(country_agg):,} rows to {output_path}")

        # 2. Region level
        logger.info("\nStep 2: Aggregating at REGION level")
        region_agg = aggregate_price(df, ["Code region"], min_sales=MIN_SALES_BY_LEVEL['region'])
        output_path = get_mart_path("region")
        region_agg.write_parquet(output_path)
        results["region"] = region_agg
        logger.info(f"✓ Saved {len(region_agg):,} rows to {output_path}")

        # 3. Department level
        logger.info("\nStep 3: Aggregating at DEPARTMENT level")
        department_agg = aggregate_price(df, ["Code departement"], min_sales=MIN_SALES_BY_LEVEL['department'])
        output_path = get_mart_path("department")
        department_agg.write_parquet(output_path)
        results["department"] = department_agg
        logger.info(f"✓ Saved {len(department_agg):,} rows to {output_path}")

        # 4. Commune level
        logger.info("\nStep 4: Aggregating at COMMUNE level")
        commune_agg = aggregate_price(df, ["Code commune"], min_sales=MIN_SALES_BY_LEVEL['commune'])
        output_path = get_mart_path("commune")
        commune_agg.write_parquet(output_path)
        results["commune"] = commune_agg
        logger.info(f"✓ Saved {len(commune_agg):,} rows to {output_path}")

        # 5. Postcode level
        logger.info("\nStep 5: Aggregating at POSTCODE level")
        postcode_agg = aggregate_price(df, ["Code postal"], min_sales=MIN_SALES_BY_LEVEL['postcode'])
        output_path = get_mart_path("postcode")
        postcode_agg.write_parquet(output_path)
        results["postcode"] = postcode_agg
        logger.info(f"✓ Saved {len(postcode_agg):,} rows to {output_path}")

        # 6. IRIS level (if available)
        if "CODE_IRIS" in df.columns:
            logger.info("\nStep 6: Aggregating at IRIS level")
            iris_agg = aggregate_price(df, ["CODE_IRIS"], min_sales=MIN_SALES_BY_LEVEL['iris'])
            output_path = get_mart_path("iris")
            iris_agg.write_parquet(output_path)
            results["iris"] = iris_agg
            logger.info(f"✓ Saved {len(iris_agg):,} rows to {output_path}")
        else:
            logger.info("\nStep 6: IRIS level - SKIPPED (CODE_IRIS column not available)")
            logger.info("Run spatial_join.py first to add IRIS codes")

        return results

    except Exception as e:
        logger.error(f"Error during aggregation: {e}", exc_info=True)
        raise


# ----------------------------
# Summary statistics
# ----------------------------

def print_aggregation_summary(results: dict[str, pl.DataFrame]) -> None:
    """Print summary statistics for all aggregation levels."""
    logger.info("\n" + "=" * 70)
    logger.info("AGGREGATION SUMMARY")
    logger.info("=" * 70)

    for level, df in results.items():
        logger.info(f"\n{level.upper()}:")
        logger.info(f"  Total areas: {df.select(pl.col('Type local').n_unique()).item():,}")
        logger.info(f"  Total rows (area × property type): {len(df):,}")

        # Property type breakdown
        type_counts = df.group_by("Type local").agg(
            n_areas=pl.len(),
            total_sales=pl.col("n_sales").sum(),
        ).sort("total_sales", descending=True)

        logger.info("  Property types:")
        for row in type_counts.iter_rows(named=True):
            logger.info(f"    - {row['Type local']}: {row['n_areas']:,} areas, {row['total_sales']:,} sales")

    logger.info("\n" + "=" * 70)


# ----------------------------
# Main execution
# ----------------------------

if __name__ == "__main__":
    try:
        # Ensure directories exist
        ensure_data_directories()

        logger.info("=" * 70)
        logger.info("REAL ESTATE PRICE AGGREGATION PIPELINE")
        logger.info("=" * 70)
        logger.info("\nMinimum sales thresholds by level:")
        for level, threshold in MIN_SALES_BY_LEVEL.items():
            logger.info(f"  {level:12s}: >= {threshold:3d} sales")
        logger.info("\nLevels: Country → Region → Department → Commune → Postcode → IRIS")

        # Run aggregation
        results = aggregate_all_levels()

        # Print summary
        print_aggregation_summary(results)

        logger.info("\n✅ All aggregations complete!")
        logger.info(f"📁 Output directory: {MART_DATA_DIR}")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)

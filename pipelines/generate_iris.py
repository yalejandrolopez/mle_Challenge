#!/usr/bin/env python3
"""
Generate IRIS-level GeoJSON tiles.
"""

import logging
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("="*70)
    logger.info("GENERATING IRIS TILES")
    logger.info("="*70)

    try:
        import geopandas as gpd
        import polars as pl

        # Load IRIS geometries
        logger.info("Loading IRIS geometries")
        iris_gdf = gpd.read_file('data/raw/contours-iris-pe.gpkg')
        logger.info(f"Loaded {len(iris_gdf):,} IRIS geometries")
        logger.info(f"CRS: {iris_gdf.crs}")

        # Filter to ONLY metropolitan France (exclude overseas territories)
        # Metropolitan France departments: 01-19, 2A, 2B, 21-95
        # Exclude: 971 (Guadeloupe), 972 (Martinique), 973 (Guyane), 974 (Réunion), 976 (Mayotte)
        logger.info("Filtering to metropolitan France only")

        # Extract department code from code_insee (first 2-3 characters)
        iris_gdf['dept'] = iris_gdf['code_insee'].str[:2]

        # Metropolitan departments (01-95 plus 2A, 2B for Corsica)
        metropolitan_depts = set([f'{i:02d}' for i in range(1, 96)]) | {'2A', '2B'}

        iris_metro = iris_gdf[iris_gdf['dept'].isin(metropolitan_depts)].copy()

        logger.info(f"Filtered from {len(iris_gdf):,} to {len(iris_metro):,} IRIS")
        logger.info(f"Removed {len(iris_gdf) - len(iris_metro):,} overseas IRIS")

        # Drop the temporary dept column
        iris_metro = iris_metro.drop(columns=['dept'])
        iris_gdf = iris_metro

        logger.debug(f"Geometry types: {iris_gdf.geometry.type.value_counts().to_dict()}")

        # Check for invalid geometries BEFORE conversion
        logger.info("Validating original geometries")
        invalid_before = (~iris_gdf.geometry.is_valid).sum()
        if invalid_before > 0:
            logger.warning(f"Found {invalid_before} invalid geometries, attempting to fix...")
            iris_gdf['geometry'] = iris_gdf.geometry.buffer(0)  # Fix invalid geometries
            invalid_after = (~iris_gdf.geometry.is_valid).sum()
            logger.info(f"Fixed, {invalid_after} invalid remaining")
        else:
            logger.info("All geometries valid")

        # Convert to WGS84 WITHOUT simplification
        logger.info("Converting to WGS84 (no simplification)")
        iris_gdf = iris_gdf.to_crs('EPSG:4326')
        logger.info(f"Converted {len(iris_gdf):,} IRIS to WGS84")

        # Load commune data
        logger.info("Loading commune aggregation")
        commune_df = pl.read_parquet('data/mart/commune.parquet')
        logger.info(f"Loaded {len(commune_df):,} commune aggregations")

        # Create INSEE mapping
        logger.info("Creating INSEE code mapping")
        dvf_clean = pl.read_parquet('data/intermediate/dvf_clean.parquet')
        insee_mapping = (
            dvf_clean
            .select(['Code commune', 'Code departement'])
            .unique()
            .with_columns(
                (pl.col('Code departement').cast(str).str.zfill(2) +
                 pl.col('Code commune').cast(str).str.zfill(3))
                .alias('code_insee')
            )
        )
        logger.info(f"✓ Created INSEE mapping for {len(insee_mapping):,} communes")

        # Join
        logger.info("\n🗺️  Joining data")
        commune_with_insee = commune_df.join(insee_mapping, on='Code commune', how='left')
        commune_pandas = commune_with_insee.to_pandas()

        logger.debug(f"Commune data shape: {commune_pandas.shape}")
        logger.debug(f"IRIS geometries: {len(iris_gdf)}")

        # IMPORTANT: Group commune data by code_insee to avoid duplicates
        # Each commune has multiple rows (one per property type)
        # We need to aggregate to avoid duplicate IRIS geometries
        logger.info("\n🔄 Deduplicating commune data")

        # Aggregate the statistics across property types
        # Also keep track of the dominant property type
        def get_dominant_type(series):
            """Get the property type with more sales"""
            if len(series) == 0:
                return 'Mixed'
            type_counts = {}
            for idx in series.index:
                prop_type = commune_pandas.loc[idx, 'Type local']
                n_sales = commune_pandas.loc[idx, 'n_sales']
                type_counts[prop_type] = type_counts.get(prop_type, 0) + n_sales
            return max(type_counts, key=type_counts.get) if type_counts else 'Mixed'

        commune_agg = commune_pandas.groupby('code_insee').agg({
            'Code commune': 'first',
            'n_sales': 'sum',  # Total sales across both types
            'median_price_m2': 'mean',  # Average median price
            'p25_price_m2': 'mean',
            'p75_price_m2': 'mean',
            'last_tx_date': 'max',
            'Type local': lambda x: get_dominant_type(x)  # Dominant property type
        }).reset_index()

        logger.info(f"Reduced to {len(commune_agg):,} unique communes")

        # Filter IRIS to only include those from communes we have data for
        logger.info("\n🔍 Filtering IRIS to communes with data")
        communes_with_data = set(commune_agg['code_insee'].values)
        iris_filtered = iris_gdf[iris_gdf['code_insee'].isin(communes_with_data)].copy()
        logger.info(f"Filtered from {len(iris_gdf):,} to {len(iris_filtered):,} IRIS")
        logger.info(f"Removed {len(iris_gdf) - len(iris_filtered):,} IRIS without data")

        # Now join - each IRIS will only match once
        iris_with_data = iris_filtered.merge(commune_agg, on='code_insee', how='inner')
        logger.info(f"✓ Joined: {len(iris_with_data):,} IRIS with data")

        if len(iris_with_data) == 0:
            logger.warning("No matches! Check data...")
            return False

        # Filter out any invalid geometries
        logger.info("\n🧹 Cleaning geometries")
        initial_count = len(iris_with_data)
        iris_with_data = iris_with_data[iris_with_data.geometry.is_valid]
        iris_with_data = iris_with_data[~iris_with_data.geometry.is_empty]
        final_count = len(iris_with_data)

        if initial_count != final_count:
            logger.info(f"Removed {initial_count - final_count} invalid geometries")
        logger.info(f"✓ {final_count:,} valid IRIS geometries")

        # Save
        logger.info("\n💾 Saving iris.geojson")
        output_path = Path('app/tiles/iris.geojson')
        output_path.parent.mkdir(parents=True, exist_ok=True)

        iris_with_data.to_file(str(output_path), driver='GeoJSON')

        size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(f"✓ Saved: {output_path} ({size_mb:.2f} MB)")

        logger.info("\n" + "="*70)
        logger.info("✅ IRIS TILES GENERATED SUCCESSFULLY!")
        logger.info("="*70)
        logger.info(f"\nFinal IRIS count: {final_count:,}")
        logger.info(f"File size: {size_mb:.2f} MB")
        return True

    except Exception as e:
        logger.error(f"ERROR: {e}", exc_info=True)
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)


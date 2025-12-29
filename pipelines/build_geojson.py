"""
Simplified vector tiles generation using Python only.

This version doesn't require tippecanoe - generates GeoJSON that can be
served directly or converted later.
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
    get_mart_path,
    PROJECT_ROOT,
)


# ----------------------------
# Config
# ----------------------------

TILES_DIR = PROJECT_ROOT / "data" / "tiles"
APP_TILES_DIR = PROJECT_ROOT / "app" / "tiles"
RAW_DIR = PROJECT_ROOT / "data" / "raw"


# ----------------------------
# Helper functions
# ----------------------------

def ensure_directories():
    """Create necessary directories for tiles."""
    TILES_DIR.mkdir(parents=True, exist_ok=True)
    APP_TILES_DIR.mkdir(parents=True, exist_ok=True)


def load_geometries_simple(level: str):
    """
    Load geometries from GeoPackage using available tools.

    Args:
        level: commune, department, region, or iris

    Returns:
        Dictionary mapping codes to geometries
    """
    logger.info(f" Loading {level.upper()} geometries")

    try:
        import geopandas as gpd

        if level == "iris":
            # Special handling for IRIS
            gpkg_path = RAW_DIR / "contours-iris-pe.gpkg"
            id_col = "code_iris"
            layer = None
        else:
            gpkg_path = RAW_DIR / "ADE_4-0_GPKG_LAMB93_FXX-ED2025-12-05.gpkg"

            if level == "commune":
                layer = "COMMUNE"
                id_col = "INSEE_COM"
            elif level == "department":
                layer = "DEPARTEMENT"
                id_col = "INSEE_DEP"
            elif level == "region":
                layer = "REGION"
                id_col = "INSEE_REG"
            else:
                raise ValueError(f"Unknown level: {level}")

        # Read GeoPackage layer
        if level == "iris":
            gdf = gpd.read_file(gpkg_path)
        else:
            gdf = gpd.read_file(gpkg_path, layer=layer)

        # Debug: Show available columns
        logger.debug(f"Available columns in {layer}: {list(gdf.columns)[:10]}")

        # Check if id_col exists, if not try to find it
        if id_col not in gdf.columns:
            logger.warning(f"Column {id_col} not found!")
            # Try to find a suitable ID column
            insee_cols = [c for c in gdf.columns if 'INSEE' in c.upper()]
            code_cols = [c for c in gdf.columns if 'CODE' in c.upper() or 'COM' in c.upper()]

            if insee_cols:
                id_col = insee_cols[0]
                logger.info(f"Using {id_col} instead")
            elif code_cols:
                id_col = code_cols[0]
                logger.info(f"Using {id_col} instead")
            else:
                logger.error(f"Cannot find suitable ID column. Available columns: {list(gdf.columns)}")
                return None, None

        # Simplify geometries for web (tolerance in meters, adjust as needed)
        logger.info("Simplifying geometries")
        # Use higher tolerance for commune level (more features = more aggressive simplification needed)
        tolerance = 200 if level == "commune" else 100
        gdf['geometry'] = gdf['geometry'].simplify(tolerance=tolerance, preserve_topology=True)

        # Convert to WGS84 (EPSG:4326) for web
        logger.info("Converting to WGS84")
        gdf = gdf.to_crs('EPSG:4326')

        logger.info(f"✓ Loaded {len(gdf):,} geometries with ID column: {id_col}")

        return gdf, id_col

    except ImportError:
        logger.error("geopandas not installed")
        logger.info("Install with: pip install geopandas")
        return None, None
    except Exception as e:
        logger.error(f"Error loading geometries: {e}", exc_info=True)
        return None, None


def create_geojson(level: str):
    """
    Create GeoJSON file by joining aggregated data with geometries.

    Args:
        level: Aggregation level (commune, department, region)

    Returns:
        Path to output GeoJSON file
    """
    logger.info(f"\n🗺️  Creating GeoJSON for {level.upper()}")

    try:
        # Load geometries
        gdf, id_col = load_geometries_simple(level)
        if gdf is None:
            return None

        # Load aggregated data
        mart_path = get_mart_path(level)
        df = pl.read_parquet(mart_path)
        logger.info(f"Loaded {len(df):,} aggregated areas")

        # For commune level, we need to construct full INSEE codes
        # since aggregated data only has commune code, not full INSEE
        if level == "commune":
            # Load the clean DVF data to get the mapping of commune + department
            from config.paths import get_dvf_clean_path
            dvf_clean = pl.read_parquet(get_dvf_clean_path())

            # Get unique commune + department mapping
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

            logger.info(f"Created INSEE code mapping for {len(insee_mapping):,} communes")

            # Join the mapping with aggregated data
            df = df.join(insee_mapping, on='Code commune', how='left')
            logger.info(f"Mapped {df.filter(pl.col('code_insee').is_not_null()).height:,} areas to INSEE codes")

        # Determine join column
        if level == "commune":
            join_key = "code_insee"  # Use the full INSEE code we just created
        elif level == "department":
            join_key = "Code departement"
        elif level == "region":
            join_key = "Code region"
        else:
            raise ValueError(f"Unknown level: {level}")

        # Convert polars to pandas for joining
        df_pandas = df.to_pandas()

        # Debug: print column names
        logger.debug(f"Geometry columns: {list(gdf.columns)[:5]}")
        logger.debug(f"Data columns: {list(df_pandas.columns)[:5]}")
        logger.debug(f"Join column in geometry: {id_col}")
        logger.debug(f"Join column in data: {join_key}")

        # Ensure the geometry ID column exists
        if id_col not in gdf.columns:
            logger.warning(f"{id_col} not found in geometry. Available columns: {list(gdf.columns)}")
            # Try to find a similar column
            possible_cols = [c for c in gdf.columns if 'INSEE' in c or 'COM' in c]
            if possible_cols:
                id_col = possible_cols[0]
                logger.info(f"Using {id_col} instead")

        # Create standardized join columns
        # The key insight: geometry uses 'code_insee', data uses 'Code departement'/'Code region'/etc
        # We need to ensure they match on the SAME column with the SAME name

        gdf_for_join = gdf.copy()

        # For department/region: rename geometry's code_insee to match the data's column name
        if level in ["department", "region"]:
            # Geometry has 'code_insee', data has 'Code departement' or 'Code region'
            # Create a common join column
            gdf_for_join['join_code'] = gdf_for_join[id_col].astype(str)
            df_pandas['join_code'] = df_pandas[join_key].astype(str)
            actual_join_key = 'join_code'
        else:
            # For commune, we already created 'code_insee' in both
            gdf_for_join['join_code'] = gdf_for_join[id_col].astype(str)
            df_pandas['join_code'] = df_pandas[join_key].astype(str)
            actual_join_key = 'join_code'

        # Debug: Show sample values
        logger.debug(f"Sample geometry IDs: {gdf_for_join['join_code'].head(3).tolist()}")
        logger.debug(f"Sample data IDs: {df_pandas['join_code'].head(3).tolist()}")

        # Join on the common column
        logger.info("Joining data with geometries")
        gdf_joined = gdf_for_join.merge(df_pandas, on=actual_join_key, how='inner')

        # Drop the temporary join column
        if 'join_code' in gdf_joined.columns:
            gdf_joined = gdf_joined.drop(columns=['join_code'])

        logger.info(f"Joined {len(gdf_joined):,} areas with geometries")

        # Select only essential columns for web visualization
        # This significantly reduces file size
        essential_columns = [
            join_key,           # Geographic identifier (Code commune, Code departement, etc)
            'Type local',       # Property type (Maison/Appartement)
            'n_sales',          # Number of transactions
            'median_price_m2',  # Median price per m²
            'p25_price_m2',     # 25th percentile
            'p75_price_m2',     # 75th percentile
            'geometry'          # Geometry (required)
        ]

        # Filter to only columns that exist
        columns_to_keep = [col for col in essential_columns if col in gdf_joined.columns]
        gdf_joined = gdf_joined[columns_to_keep]

        logger.info(f"Reduced to {len(columns_to_keep)} essential columns")

        # Save as GeoJSON
        output_path = APP_TILES_DIR / f"{level}.geojson"
        gdf_joined.to_file(output_path, driver='GeoJSON')

        # Get file size
        size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(f"✓ Saved {output_path.name} ({size_mb:.2f} MB)")

        return output_path

    except Exception as e:
        logger.error(f"Error creating GeoJSON for {level}: {e}", exc_info=True)
        return None


# ----------------------------
# Main pipeline
# ----------------------------

def build_geojson_tiles(levels: list[str] | None = None):
    """
    Build GeoJSON files for specified levels.

    Args:
        levels: List of levels to process (default: ['commune'])
    """
    try:
        logger.info("=" * 70)
        logger.info("GEOJSON TILES GENERATION (Python-only)")
        logger.info("=" * 70)

        # Ensure directories exist
        ensure_directories()

        # Default to commune only
        if levels is None:
            levels = ['commune']

        logger.info(f"\n📍 Processing levels: {', '.join(levels)}")

        results = {}

        for level in levels:
            geojson_path = create_geojson(level)
            if geojson_path:
                results[level] = geojson_path

        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("GEOJSON GENERATION SUMMARY")
        logger.info("=" * 70)

        if results:
            for level, path in results.items():
                size_mb = path.stat().st_size / (1024 * 1024)
                logger.info(f"  ✓ {level.upper()}: {path.name} ({size_mb:.2f} MB)")

            logger.info("\n" + "=" * 70)
            logger.info("✅ GeoJSON files generated!")
            logger.info(f"📁 Files saved to: {APP_TILES_DIR}")
            logger.info("\n💡 Next step:")
            logger.info("  1. Update app/index.html to use GeoJSON instead of PMTiles")
            logger.info("  2. Or install tippecanoe to convert to PMTiles:")
            logger.info("     brew install tippecanoe")
            logger.info("     tippecanoe -o commune.pmtiles commune.geojson")
        else:
            logger.error("No files generated. Install geopandas:")
            logger.info("   pip install geopandas")

        logger.info("=" * 70)

        return results

    except Exception as e:
        logger.error(f"Error building GeoJSON tiles: {e}", exc_info=True)
        return {}


# ----------------------------
# Main execution
# ----------------------------

if __name__ == "__main__":
    # Generate multiple levels for zoom-based switching
    build_geojson_tiles(['commune', 'department', 'region'])


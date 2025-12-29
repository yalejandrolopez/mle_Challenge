"""
Vector tiles generation pipeline

Generates PMTiles for efficient web mapping visualization:
1. Load aggregated parquet data from mart/
2. Join with geometries from GeoPackage/GeoJSON
3. Export to GeoJSON
4. Generate PMTiles with tippecanoe

Outputs:
- GeoJSON intermediate files (data/tiles/*.geojson)
- PMTiles for web (app/tiles/*.pmtiles)
"""

from __future__ import annotations

import sys
import subprocess
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

# Layer configuration
LEVELS_CONFIG = {
    "commune": {
        "gpkg": "ADE_4-0_GPKG_LAMB93_FXX-ED2025-12-05.gpkg",
        "layer": "COMMUNE",
        "join_col": "INSEE_COM",
        "zoom_range": "9-12",
        "min_zoom": 9,
        "max_zoom": 12,
    },
    "department": {
        "gpkg": "ADE_4-0_GPKG_LAMB93_FXX-ED2025-12-05.gpkg",
        "layer": "DEPARTEMENT",
        "join_col": "INSEE_DEP",
        "zoom_range": "7-9",
        "min_zoom": 7,
        "max_zoom": 9,
    },
    "region": {
        "gpkg": "ADE_4-0_GPKG_LAMB93_FXX-ED2025-12-05.gpkg",
        "layer": "REGION",
        "join_col": "INSEE_REG",
        "zoom_range": "5-7",
        "min_zoom": 5,
        "max_zoom": 7,
    },
}


# ----------------------------
# Helper functions
# ----------------------------

def ensure_directories():
    """Create necessary directories for tiles."""
    TILES_DIR.mkdir(parents=True, exist_ok=True)
    APP_TILES_DIR.mkdir(parents=True, exist_ok=True)


def check_dependencies():
    """Check if required tools are installed."""
    logger.info("Checking dependencies")

    # Check ogr2ogr (GDAL)
    try:
        result = subprocess.run(["ogr2ogr", "--version"],
                              capture_output=True, text=True, check=False)
        if result.returncode == 0:
            logger.info("ogr2ogr (GDAL) found")
        else:
            logger.error("ogr2ogr not found")
            logger.info("Install with: brew install gdal")
            return False
    except FileNotFoundError:
        logger.error("ogr2ogr not found")
        logger.info("Install with: brew install gdal")
        return False

    # Check tippecanoe
    try:
        result = subprocess.run(["tippecanoe", "--version"],
                              capture_output=True, text=True, check=False)
        if result.returncode == 0:
            logger.info("tippecanoe found")
        else:
            logger.error("tippecanoe not found")
            logger.info("Install with: brew install tippecanoe")
            return False
    except FileNotFoundError:
        logger.error("tippecanoe not found")
        logger.info("Install with: brew install tippecanoe")
        return False

    return True


def join_with_geometry(level: str, config: dict) -> Path:
    """
    Join aggregated data with geometry using ogr2ogr.

    Args:
        level: Aggregation level (commune, department, region)
        config: Level configuration

    Returns:
        Path to output GeoJSON file
    """
    try:
        logger.info(f"\n📊 Processing {level.upper()} level")

        # Load aggregated data
        mart_path = get_mart_path(level)
        df = pl.read_parquet(mart_path)
        logger.info(f"Loaded {len(df):,} aggregated areas")

        # Prepare join key based on level
        if level == "commune":
            join_key = "Code commune"
        elif level == "department":
            join_key = "Code departement"
        elif level == "region":
            join_key = "Code region"
        else:
            raise ValueError(f"Unknown level: {level}")

        # Export aggregated data as CSV for joining
        csv_path = TILES_DIR / f"{level}_data.csv"
        df.write_csv(csv_path)
        logger.info(f"✓ Saved data to {csv_path.name}")

        # Path to geometry file
        gpkg_path = PROJECT_ROOT / "data" / "raw" / config["gpkg"]

        # Output GeoJSON path
        geojson_path = TILES_DIR / f"{level}.geojson"

        # Use ogr2ogr to join geometry with data
        logger.info(f"🔗 Joining with geometries from {config['layer']}")

        # Simple approach: export geometries first, then join in Python
        temp_geojson = TILES_DIR / f"{level}_geom.geojson"

        cmd = [
            "ogr2ogr",
            "-f", "GeoJSON",
            str(temp_geojson),
            str(gpkg_path),
            config["layer"],
            "-select", config["join_col"],
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Error exporting geometries: {result.stderr}")
            return None

        logger.info(f"✓ Exported geometries to {temp_geojson.name}")

        # Now join using geopandas (if available) or create a simplified version
        try:
            import geopandas as gpd

            # Load geometry
            gdf = gpd.read_file(temp_geojson)

            # Load data
            df_pandas = df.to_pandas()

            # Rename join column in geodataframe to match
            gdf = gdf.rename(columns={config["join_col"]: join_key})

            # Join
            gdf_joined = gdf.merge(df_pandas, on=join_key, how="inner")

            # Select relevant columns
            cols = [join_key, "Type local", "n_sales", "median_price_m2",
                    "p25_price_m2", "p75_price_m2", "last_tx_date", "geometry"]
            gdf_joined = gdf_joined[[c for c in cols if c in gdf_joined.columns]]

            # Save to GeoJSON
            gdf_joined.to_file(geojson_path, driver="GeoJSON")
            logger.info(f"✓ Created {geojson_path.name} with {len(gdf_joined):,} features")

        except ImportError:
            logger.warning("geopandas not available, using simplified approach")
            # Fallback: just copy the geometry file for now
            import shutil
            shutil.copy(temp_geojson, geojson_path)
            logger.info(f"✓ Created {geojson_path.name} (geometries only)")

        return geojson_path

    except Exception as e:
        logger.error(f"Error joining geometry for {level}: {e}", exc_info=True)
        return None


def generate_pmtiles(level: str, geojson_path: Path, config: dict) -> Path:
    """
    Generate PMTiles from GeoJSON using tippecanoe.

    Args:
        level: Aggregation level
        geojson_path: Path to input GeoJSON
        config: Level configuration

    Returns:
        Path to output PMTiles file
    """
    try:
        logger.info(f"\n🗺️  Generating PMTiles for {level}")

        output_path = APP_TILES_DIR / f"{level}.pmtiles"

        cmd = [
            "tippecanoe",
            "-o", str(output_path),
            "--force",  # Overwrite if exists
            "-Z", str(config["min_zoom"]),
            "-z", str(config["max_zoom"]),
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            "-l", level,  # Layer name
            str(geojson_path),
        ]

        logger.info("Running tippecanoe")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"Error generating PMTiles: {result.stderr}")
            return None

        # Get file size
        size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(f"✓ Created {output_path.name} ({size_mb:.2f} MB)")

        return output_path

    except Exception as e:
        logger.error(f"Error generating PMTiles for {level}: {e}", exc_info=True)
        return None


# ----------------------------
# Main pipeline
# ----------------------------

def build_tiles(levels: list[str] | None = None):
    """
    Build vector tiles for specified levels.

    Args:
        levels: List of levels to process (default: all configured levels)
    """
    try:
        logger.info("=" * 70)
        logger.info("VECTOR TILES GENERATION PIPELINE")
        logger.info("=" * 70)

        # Ensure directories exist
        ensure_directories()

        # Check dependencies
        if not check_dependencies():
            logger.error("Missing dependencies. Please install required tools")
            logger.info("\nFor macOS:")
            logger.info("  brew install gdal")
            logger.info("  brew install tippecanoe")
            return

        # Default to all levels
        if levels is None:
            levels = list(LEVELS_CONFIG.keys())

        logger.info(f"\n📍 Processing levels: {', '.join(levels)}")

        results = {}

        for level in levels:
            if level not in LEVELS_CONFIG:
                logger.warning(f"Unknown level: {level}, skipping")
                continue

            config = LEVELS_CONFIG[level]

            # Step 1: Join with geometry
            geojson_path = join_with_geometry(level, config)
            if geojson_path is None:
                continue

            # Step 2: Generate PMTiles
            pmtiles_path = generate_pmtiles(level, geojson_path, config)
            if pmtiles_path is None:
                continue

            results[level] = pmtiles_path

        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("TILES GENERATION SUMMARY")
        logger.info("=" * 70)

        for level, path in results.items():
            size_mb = path.stat().st_size / (1024 * 1024)
            zoom = LEVELS_CONFIG[level]["zoom_range"]
            logger.info(f"  ✓ {level.upper()}: {path.name} ({size_mb:.2f} MB, zoom {zoom})")

        logger.info("\n" + "=" * 70)
        logger.info("✅ Vector tiles generation complete!")
        logger.info(f"📁 Tiles saved to: {APP_TILES_DIR}")
        logger.info("\n💡 Next step: Open app/index.html in a browser")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"Error in tiles generation pipeline: {e}", exc_info=True)
        raise


# ----------------------------
# Main execution
# ----------------------------

if __name__ == "__main__":
    try:
        # Start with commune only (biggest impact)
        build_tiles(["commune"])

        # Uncomment to process all levels:
        # build_tiles()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


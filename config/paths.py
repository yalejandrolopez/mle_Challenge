"""
Centralized path management for the mle_Challenge project.

All file paths used across pipelines should be defined here to ensure
consistency and make path management easier.
"""

from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.resolve()

# Main data directories
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
INTERMEDIATE_DATA_DIR = DATA_DIR / "intermediate"
MART_DATA_DIR = DATA_DIR / "mart"

# Pipeline directory
PIPELINES_DIR = PROJECT_ROOT / "pipelines"

# App directory
APP_DIR = PROJECT_ROOT / "app"

# Raw data files
DVF_RAW_FILE = RAW_DATA_DIR / "ValeursFoncieres-2025-S1.txt"

# Geometry files
ADMIN_BOUNDARIES_FILE = RAW_DATA_DIR / "ADE_4-0_GPKG_LAMB93_FXX-ED2025-12-05.gpkg"
IRIS_BOUNDARIES_FILE = RAW_DATA_DIR / "contours-iris-pe.gpkg"
POSTCODE_HEXASMAL_FILE = RAW_DATA_DIR / "laposte-hexasmal.geojson"

# Intermediate data files
DVF_CLEAN_FILE = INTERMEDIATE_DATA_DIR / "dvf_clean.parquet"
DVF_WITH_GEOMETRIES_FILE = INTERMEDIATE_DATA_DIR / "dvf_with_geometries.parquet"

# Mart data files (aggregated by level)
MART_COUNTRY_FILE = MART_DATA_DIR / "country.parquet"
MART_REGION_FILE = MART_DATA_DIR / "region.parquet"
MART_DEPARTMENT_FILE = MART_DATA_DIR / "department.parquet"
MART_COMMUNE_FILE = MART_DATA_DIR / "commune.parquet"
MART_POSTCODE_FILE = MART_DATA_DIR / "postcode.parquet"
MART_IRIS_FILE = MART_DATA_DIR / "iris.parquet"


def ensure_data_directories():
    """
    Create all data directories if they don't exist.
    """
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    INTERMEDIATE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    MART_DATA_DIR.mkdir(parents=True, exist_ok=True)


def get_dvf_raw_path() -> Path:
    """Get the path to the raw DVF data file."""
    return DVF_RAW_FILE


def get_dvf_clean_path() -> Path:
    """Get the path to the cleaned DVF data file."""
    return DVF_CLEAN_FILE


def get_dvf_with_geometries_path() -> Path:
    """Get the path to the DVF data enriched with geometries."""
    return DVF_WITH_GEOMETRIES_FILE


def get_admin_boundaries_path() -> Path:
    """Get the path to administrative boundaries GeoPackage."""
    return ADMIN_BOUNDARIES_FILE


def get_iris_boundaries_path() -> Path:
    """Get the path to IRIS boundaries GeoPackage."""
    return IRIS_BOUNDARIES_FILE


def get_postcode_hexasmal_path() -> Path:
    """Get the path to postcode hexasmal GeoJSON."""
    return POSTCODE_HEXASMAL_FILE


def get_mart_path(level: str) -> Path:
    """
    Get the path to a mart file for a specific aggregation level.

    Args:
        level: One of 'country', 'region', 'department', 'commune', 'postcode', 'iris'

    Returns:
        Path to the mart file
    """
    level_map = {
        "country": MART_COUNTRY_FILE,
        "region": MART_REGION_FILE,
        "department": MART_DEPARTMENT_FILE,
        "commune": MART_COMMUNE_FILE,
        "postcode": MART_POSTCODE_FILE,
        "iris": MART_IRIS_FILE,
    }
    if level not in level_map:
        raise ValueError(f"Unknown level: {level}. Must be one of {list(level_map.keys())}")
    return level_map[level]


if __name__ == "__main__":
    # Print all paths for verification
    print("Project paths:")
    print(f"  PROJECT_ROOT: {PROJECT_ROOT}")
    print(f"  DATA_DIR: {DATA_DIR}")
    print(f"  RAW_DATA_DIR: {RAW_DATA_DIR}")
    print(f"  INTERMEDIATE_DATA_DIR: {INTERMEDIATE_DATA_DIR}")
    print(f"  MART_DATA_DIR: {MART_DATA_DIR}")
    print(f"\nRaw data files:")
    print(f"  DVF_RAW_FILE: {DVF_RAW_FILE}")
    print(f"  ADMIN_BOUNDARIES_FILE: {ADMIN_BOUNDARIES_FILE}")
    print(f"  IRIS_BOUNDARIES_FILE: {IRIS_BOUNDARIES_FILE}")
    print(f"  POSTCODE_HEXASMAL_FILE: {POSTCODE_HEXASMAL_FILE}")
    print(f"\nIntermediate files:")
    print(f"  DVF_CLEAN_FILE: {DVF_CLEAN_FILE}")
    print(f"  DVF_WITH_GEOMETRIES_FILE: {DVF_WITH_GEOMETRIES_FILE}")
    print(f"\nMart files:")
    for level in ["country", "region", "department", "commune", "postcode", "iris"]:
        print(f"  {level}: {get_mart_path(level)}")


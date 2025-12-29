"""
DVF (Demandes de Valeurs Foncières) cleaning pipeline
- Robust CSV parsing (DVF has mixed-type columns like "1er lot" = "12C")
- Keeps only residential sales (Maison, Appartement)
- Collapses multiple rows per mutation to a single "main local"
- Computes robust €/m² at transaction level

Usage:
    df_clean = build_clean_transactions(".../ValeursFoncieres-2025-S1.txt")
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
from config.paths import get_dvf_raw_path, get_dvf_clean_path, ensure_data_directories


# ----------------------------
# Config
# ----------------------------

RESIDENTIAL_TYPES = {"Maison", "Appartement"}

MIN_SURFACE_M2 = 8.0
MIN_PRICE_EUR = 10_000.0
# Initial bounds for obvious errors
PRICE_M2_INITIAL_BOUNDS = (300.0, 30_000.0)
# Will apply percentile-based outlier removal after initial filter

DVF_SEPARATOR = "|"
DVF_ENCODING = "utf8"
DVF_NULL_VALUES = ["", "NA"]



# ----------------------------
# Helpers
# ----------------------------

def read_dvf_columns(path: str | Path, sep: str = DVF_SEPARATOR) -> list[str]:
    """
    Read only the header line without triggering type inference.
    This avoids Polars crashing on mixed-type columns (e.g., '1er lot' has '12C').
    """
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        header = f.readline().rstrip("\n")
    return header.split(sep)


def build_string_schema_from_header(path: str | Path) -> dict[str, pl.DataType]:
    """
    Force ALL columns to Utf8. DVF frequently contains alphanumeric values in columns
    that look numeric early in the file (e.g., "12C").
    """
    cols = read_dvf_columns(path)
    return {c: pl.Utf8 for c in cols}


def parse_float_fr(expr: pl.Expr) -> pl.Expr:
    """
    Parse French-formatted numbers:
    - strips whitespace
    - comma decimal -> dot
    - casts to Float64 (non-parseable -> null)
    """
    return (
        expr.cast(pl.Utf8)
        .str.replace_all(r"\s+", "")
        .str.replace(",", ".")
        .cast(pl.Float64, strict=False)
    )


# ----------------------------
# Load
# ----------------------------

def load_dvf(path: str | Path) -> pl.DataFrame:
    """
    Load DVF CSV robustly:
    - all columns as strings (Utf8)
    - only cast numeric columns needed by the pipeline
    """
    path = Path(path)
    schema = build_string_schema_from_header(path)

    df = pl.read_csv(
        path,
        separator=DVF_SEPARATOR,
        encoding=DVF_ENCODING,
        null_values=DVF_NULL_VALUES,
        schema_overrides=schema,
        ignore_errors=True,  # keep going if occasional malformed lines exist
    )

    return df.with_columns(
        [
            parse_float_fr(pl.col("Valeur fonciere")).alias("price_eur"),
            parse_float_fr(pl.col("Surface reelle bati")).alias("surface_bati"),
            parse_float_fr(pl.col("Surface terrain")).alias("surface_terrain"),
            parse_float_fr(pl.col("Surface Carrez du 1er lot")).alias("surface_carrez"),
        ]
    )


# ----------------------------
# Transform
# ----------------------------

def add_mutation_id(df: pl.DataFrame) -> pl.DataFrame:
    """
    Build a stable mutation key. DVF does not always provide a unique transaction id.
    This is a pragmatic composite key suitable for grouping.
    """
    cols = [
        "Identifiant de document",
        "Date mutation",
        "Valeur fonciere",
        "Code departement",
        "Code commune",
        "Section",
        "No plan",
    ]

    # If any of these columns are missing in a given DVF export, fail fast with a clear message
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"Missing expected DVF columns for mutation_id: {missing}")

    return df.with_columns(
        pl.concat_str([pl.col(c).fill_null("") for c in cols], separator="|").alias("mutation_id")
    )


def filter_residential_sales(df: pl.DataFrame) -> pl.DataFrame:
    """
    Keep only existing residential sales:
    - Nature mutation == 'Vente'
    - Type local in {Maison, Appartement}
    - Surface: prefer surface_bati; fallback to Carrez
    """
    required = ["Nature mutation", "Type local", "price_eur", "surface_bati", "surface_carrez"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing expected DVF columns for residential filter: {missing}")

    return (
        df.filter(pl.col("Nature mutation") == "Vente")
        .filter(pl.col("Type local").is_in(list(RESIDENTIAL_TYPES)))
        .with_columns(
            pl.when(pl.col("surface_bati").is_not_null() & (pl.col("surface_bati") > 0))
            .then(pl.col("surface_bati"))
            .otherwise(pl.col("surface_carrez"))
            .alias("surface_final")
        )
        .filter(pl.col("surface_final") > MIN_SURFACE_M2)
        .filter(pl.col("price_eur") > MIN_PRICE_EUR)
    )


def select_main_local(df: pl.DataFrame) -> pl.DataFrame:
    """
    DVF often has multiple rows per mutation (annexes, multiple parcels, etc.).
    Select one representative residential 'main local' per mutation:
      - choose the row with the largest surface_final
    """
    return (
        df.sort("surface_final", descending=True)
        .unique(subset=["mutation_id"], keep="first")
    )


def compute_price_m2(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute €/m² and apply plausibility bounds with robust outlier removal.

    Strategy:
    1. Apply initial bounds to remove obvious errors
    2. Calculate percentiles (p10, p90)
    3. Remove extreme outliers beyond IQR-based bounds to prevent luxury properties
       from skewing aggregations (especially important for Paris)
    """
    low_initial, high_initial = PRICE_M2_INITIAL_BOUNDS

    # Step 1: Compute price_m2 and apply initial bounds
    df_with_price = df.with_columns(
        (pl.col("price_eur") / pl.col("surface_final")).alias("price_m2")
    ).filter(pl.col("price_m2").is_between(low_initial, high_initial))

    # Step 2: Calculate percentiles for robust outlier detection
    p10 = df_with_price.select(pl.col("price_m2").quantile(0.10)).item()
    p90 = df_with_price.select(pl.col("price_m2").quantile(0.90)).item()

    # Use p90 + 1.5 * IQR as upper bound (captures luxury but excludes extreme outliers)
    # This is more conservative than fixed €30k limit
    iqr = p90 - p10
    upper_bound = p90 + 1.5 * iqr

    # Cap at €15,000/m² as absolute maximum (luxury Paris apartments rarely exceed this)
    upper_bound = min(upper_bound, 15000.0)

    logger.info(f"Price/m² filtering: {low_initial:.0f} - {upper_bound:.0f} (p10={p10:.0f}, p90={p90:.0f})")

    # Step 3: Apply robust bounds
    return df_with_price.filter(pl.col("price_m2") <= upper_bound)


# ----------------------------
# Public API
# ----------------------------

def build_clean_transactions(path: str | Path) -> pl.DataFrame:
    """
    End-to-end: DVF raw -> clean transaction-level €/m² table.
    Output: 1 row per mutation (representative main residential local).
    """
    return (
        load_dvf(path)
        .pipe(add_mutation_id)
        .pipe(filter_residential_sales)
        .pipe(select_main_local)
        .pipe(compute_price_m2)
    )


# ----------------------------
# Example run
# ----------------------------

if __name__ == "__main__":
    try:
        # Ensure data directories exist
        ensure_data_directories()

        # Get paths from centralized config
        dvf_path = get_dvf_raw_path()
        logger.info(f"Processing file: {dvf_path}")

        # Run the cleaning pipeline
        df_clean = build_clean_transactions(dvf_path)

        # Quick sanity checks
        n_transactions = df_clean.select(pl.len().alias('n_transactions')).item()
        logger.info(f"Number of transactions: {n_transactions:,}")

        stats = df_clean.select(
            pl.col("price_m2").median().alias("median_price_m2"),
            pl.col("price_m2").quantile(0.9).alias("p90_price_m2"),
        )
        logger.info(f"Median price/m²: €{stats['median_price_m2'][0]:.2f}")
        logger.info(f"P90 price/m²: €{stats['p90_price_m2'][0]:.2f}")

        # Save the cleaned data
        output_path = get_dvf_clean_path()
        df_clean.write_parquet(output_path)
        logger.info(f"✅ Cleaned data saved to: {output_path}")

    except Exception as e:
        logger.error(f"Error in DVF cleaning pipeline: {e}", exc_info=True)
        sys.exit(1)

# MLE Challenge – Residential Property Price Mapping (France)

This project analyzes residential property transactions in France to estimate current market prices expressed as price per square meter (€/m²).
It delivers a multi-level aggregation pipeline and an interactive map visualization designed for performance, robustness, and clarity of assumptions.

---

## Project Scope

The objective is to compute and visualize aggregated residential property prices at multiple geographic levels using open data published by the French government.

### Included aggregation levels
- Country
- Region
- Department
- Commune (city)
- Postcode
- Neighborhood 
- The architecture supports per-department tiling and on-demand conversion for production-scale deployments.
- This design choice is documented and intentional, prioritizing usability, performance, and statistical robustness.

---

## Data Sources

- DVF (Demandes de Valeurs Foncières)  
  Public transaction-level real estate data provided by the French government.
- DVF (Demandes de Valeurs Foncières)  
  Public transaction-level real estate data provided by the French government.
- DVF (Demandes de Valeurs Foncières)  
  Public transaction-level real estate data provided by the French government.
- DVF (Demandes de Valeurs Foncières)  
  Public transaction-level real estate data provided by the French government.
- DVF (Demandes de Valeurs Foncières)  
  Public transaction-level real estate data provided by the French government.
  Polygon approximations for postal code aggregation.
- Administrative boundaries (ADMIN EXPRESS – IGN)  
  Used for country, region, department, and commune geometries.
  Polygon approximations for postal code aggregation.
- IRIS boundaries  
  Standardized neighborhood-level statistical units (used when coordinates are available).
python3 -m http.server 8000
- Postcode boundaries (La Poste / Etalab)  
  Polygon approximations for postal code aggregation.
### Minimum Sales Thresholds (Stability Rule)

Different thresholds are applied per level to ensure statistical reliability:
---
This approach:
- Reduces statistical noise at higher aggregation levels
- Limits disclosure risk for individual properties
- Improves reliability of displayed values
- Provides better coverage at granular levels where users need detail
   - Source: [data.gouv.fr - Contours IRIS](https://www.data.gouv.fr/fr/datasets/contours-iris/)
Areas with fewer transactions are excluded from visualization at that level.

**Outputs:**
```
data/mart/
├── country.parquet
├── region.parquet
├── department.parquet
├── commune.parquet
├── postcode.parquet
└── iris.parquet (if IRIS data available)
```

Each file contains aggregated statistics per geography × property type combination.
   - File: `laposte-hexasmal.geojson`
   - Polygon approximations for postal codes

**Script:** `pipelines/build_geojson.py`

All datasets used are open data and suitable for redistribution.


### Full Pipeline

Run all steps automatically:

```bash
python3 complete_regeneration.py
```

This executes:
1. `pipelines/aggregate.py` - Generate all aggregation levels
2. `pipelines/build_geojson.py` - Create region/department/commune tiles
3. `pipelines/generate_iris.py` - Create IRIS-level tiles

Total runtime: ~5-15 minutes depending on hardware.

### Individual Steps

Run specific pipeline stages:

```bash
# Clean transactions
python3 pipelines/clean_dvf.py

# Generate aggregations
python3 pipelines/aggregate.py

# Build web tiles
python3 pipelines/build_geojson.py

# Generate IRIS tiles
python3 pipelines/generate_iris.py
```

### Quick Regeneration

If aggregation files already exist, quickly regenerate tiles:

```bash
python3 force_regenerate.py
```

This skips aggregation and only rebuilds GeoJSON tiles from existing parquet files.

### Current Limitations

1. **Parcel-level visualization** - Not included due to data format constraints (EDIGÉO/DXF) and performance considerations. A scalable design would require:
   - Per-department tile generation
   - Vector tile format (PMTiles/MBTiles)
   - Significant storage and preprocessing

2. **IRIS implementation** - Currently uses commune-level statistics aggregated to IRIS boundaries. Full implementation would require:
   - Transaction-level coordinate-based spatial join
   - More complex geometry handling
   - Additional processing time

3. **Rural/low-activity areas** - Some areas exhibit higher uncertainty due to limited transaction volumes. While minimum thresholds are applied, additional statistical treatment could improve confidence estimation.

4. **Temporal coverage** - Current version uses single time period (2025-S1). Historical trends not yet implemented.

5. **Data downloads** - Manual download required (no automated fetching). 

### Future Extensions

Potential improvements for production deployment:

- **Statistical enhancements:**
  - Temporal weighting of transactions (recency-weighted estimates)
  - Confidence intervals based on sample size

- **Temporal analysis:**
  - n_sales
  - Seasonal adjustment
  - Price growth rates

---

## Technical Notes

### Dependencies

- **polars**  (faster than pandas)
- **geopandas** - Geospatial operations
- **pyogrio** - GeoPackage reading 


## GeoJSON Tile Generation

For web visualization, simplified GeoJSON tiles are generated:

```bash
python3 pipelines/build_geojson.py

```
### Stability rule
Only areas with at least 10 transactions are kept. This reduces statistical noise, and improves the reliability of displayed values.
Areas with fewer transactions are considered high-uncertainty and are excluded from visualization at that level.

Output titles:
```
app/titles/
├── commune.geojson
├── department.geojson
├── region.geojson
└── iris.geojson
```

---

### Run locally

```python
cd app
python3 -m http.server 8000
```

---


## Limitations and Future Work

- Parcel-level visualization is not included due to data format constraints and performance considerations. A scalable design is documented.

- Some rural or low-activity geometries exhibit higher uncertainty. While basic filtering is applied, deeper statistical treatment could improve confidence estimation.

---

## Conclusion.

This project delivers a robust, well-documented pipeline for estimating and visualizing residential property prices in France.
The design prioritizes statistic robustness (when possible), performance, and transparency of assumptions.

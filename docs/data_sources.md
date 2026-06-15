# Data Sources

## FEMA NFIP Redacted Claims (real)

- **Source:** [FEMA OpenFEMA — FIMA NFIP Redacted Claims v2](https://www.fema.gov/openfema-data-page/fima-nfip-redacted-claims-v2)
- **License:** U.S. public domain
- **Total dataset**: ~2.6M rows (per FEMA)
- **Format:** CSV (also available via OpenFEMA REST API for incremental pulls)
- **Time range**: 1978-01-08 to 2026-05-30 (48 years)
- Geographic spread: 50+ states, concentrated in LA (18%), FL (17%), TX (14%)

## Data quality issues observed

### Sentinel values
- `originalConstructionDate` = 1492-10-12 means "unknown construction date" (~X% of rows). Silver rule: dates before 1900 → null.
- `reportedCity` = "Currently Unavailable" is FEMA's privacy redaction at the city level. Silver rule: treat as null.

### Type inconsistencies
- `rateMethod` mixes numeric codes (1, 9) with strings ("RatingEngine"). Treated as STRING in bronze, kept as-is.
- Boolean indicator fields encoded as 0/1 integers; cast to BOOLEAN in silver.
- Categorical code columns (occupancyType, causeOfDamage, condominiumCoverageTypeCode, locationOfContents) require reference table lookups for human-readable labels.

### Sparse and conditional fields
- `amountPaidOnContentsClaim` and other payment columns: blanks indicate unknown, 0 indicates real zero (no claim or denied). Silver rule: blanks → null, preserve real zeros.
- Some rows have minimal data (likely denied claims or incomplete records); preserved in silver, with NULLs documented.

### Versioned attributes
- `ratedFloodZone` vs `floodZoneCurrent`: flood zone at time of rating vs current FEMA-designated zone. Both retained; FEMA periodically remaps zones.
- `nfipRatedCommunityNumber` vs `nfipCommunityNumberCurrent`: same pattern for NFIP community codes.

### Sample-confirmed
- `id` is a UUID per claim, fully populated, unique → degenerate dimension `fema_claim_id`.
- `floodEvent` is human-readable event name where populated; null/blank for non-CAT claims → drives dim_cat_event derivation.
- Geographic granularity reaches lat/long and census block group; will note PII implications in decisions log.


## Data profile (50k row sample)

### Volume
- Total dataset: ~2.6M rows (per FEMA)
- Sample profiled: 50,000 rows
- Time range: 1978-01-08 to 2026-05-30 (48 years)
- Geographic spread: 50+ states, concentrated in LA (18%), FL (17%), TX (14%)

### Key uniqueness and completeness
- `id` (UUID): 100% populated, 100% unique → degenerate dimension `fema_claim_id`
- `dateOfLoss`: 100% populated
- `floodEvent`: ~80% populated; 180 unique values (drives dim_cat_event size)

### Payment column patterns
- ~21% of rows (10,588) have null payment data across all amount columns — denied/open/incomplete records, preserved with NULLs
- Building payments: 73% positive, 5% zero (no building damage), 21% null
- Contents payments: 36% positive, 43% zero (no contents coverage or damage), 21% null
- ICC payments: 1.5% positive (only triggers on substantial structural rebuilds)
- **Silver rule:** preserve NULL vs 0 distinction; do not coerce to zero

### Sentinel values confirmed in data
- `originalConstructionDate` = 1492-10-12: **12.3%** of rows (6,157/50,000) — unknown construction date sentinel
- `reportedCity` = "Currently Unavailable": **all rows in sample** — privacy redaction; drop column from dim_geography
- `floodEvent` = "Flooding" or "Not a named storm": background claims not tied to named CAT events; represent in dim_cat_event with event_type = 'UNNAMED' rather than NULL

### Categorical / boolean encoding
- All `*Indicator` columns are 0/1 INT → cast BOOLEAN in silver
- `floodproofedIndicator` near-constant (0.01% positive) — keep but expect skew
- `rateMethod` mixes integer codes with string "RatingEngine" → treat as STRING

### Top flood events in sample (gives dim_cat_event preview)
| Event | Sample rows |
|-------|-------------|
| Flooding (unnamed) | 6,589 |
| Hurricane Katrina | 3,710 |
| Hurricane Sandy | 2,554 |
| Hurricane Harvey | 1,626 |
| Hurricane Irene | 1,340 |
| Hurricane Ike | 1,097 |
| Hurricane Helene | 1,040 |
| Hurricane Ian | 915 |
| Tropical Storm Allison | 765 |
| Hurricane Irma | 642 |
| ... | ... |

Full 180-event list will be derived from full dataset in bronze→silver pipeline.

### State concentration (sample)
LA 18%, FL 17%, TX 14%, NJ 7%, NY 6% — claims cluster by major catastrophes. Top 5 states = 62% of claims; useful for dashboard geographic emphasis.
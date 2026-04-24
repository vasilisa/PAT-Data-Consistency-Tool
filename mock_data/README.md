# Mock Data Pack for PAT Data Consistency Tool

This folder contains synthetic CSV datasets aligned to the v1 spec.

## Files

- tbl_DetailedData.csv
- tbl_Key_Mapping.csv
- tbl_RateChange.csv
- tbl_RateChange_Pol.csv
- tbl_Trend.csv
- tbl_Patterns_Attr.csv
- tbl_Patterns_Large.csv
- tbl_Patterns_Prem.csv
- tbl_Forecast_Adjustment.csv

## Design Intent

The data is intentionally mixed so the webapp should produce both FAIL and WARNING results:

1. Key registration
- Should generally pass for listed keys.
- Key_Forecast appears in a reference table and is intentionally not in mapping checks scope per spec exception.

2. Referential integrity
- Forward FAIL expected:
  - Key_RateChange = RC_MISS exists in DetailedData but not in tbl_RateChange.
  - Key_Trend = TR_MISS exists in DetailedData but not in tbl_Trend.
- Reverse WARNING expected:
  - RC_ORPHAN exists in tbl_RateChange but not in DetailedData.
  - TR_ORPHAN exists in tbl_Trend but not in DetailedData.

3. Parent column rule
- tbl_Key_Mapping intentionally maps Key_RateChange RC_A to multiple Region/LOB values.
- This should trigger parent-rule failures for affected candidate columns depending on implementation details.

4. Mapping table uniqueness
- Duplicate non-key combination is included for Segment=S2, Region=South, LOB=Motor.
- Should trigger FAIL for mapping uniqueness.

5. Value checks
- RateChange values include large magnitudes (0.90, -0.80, 0.60, -0.55) to trigger warning/fail thresholds.
- Trend_Value includes one moderate outlier (0.40) to trigger a warning band.
- DevPct_* includes values above 1 and high means in some columns.
- DevM sequences intentionally violate start/gap/max rules.
- AsAt_Month includes invalid month 202613.

## Usage in Dataiku

1. Upload each CSV as a dataset with the same table name as the file (without .csv).
2. Ensure types are inferred as expected:
   - Premium, DevM, RateChange, Trend_Value, DevPct_* as numeric
   - AsAt_Month as integer or string (your check should validate format either way)
3. Run the consistency webapp and compare outcomes against the design intent above.

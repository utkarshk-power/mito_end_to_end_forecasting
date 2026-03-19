# Drift Detection README

## Purpose

This document captures the current design intent for drift detection in
`appletree_end_to_end_forecasting` so future sessions can recover context
quickly.

## Repo Context

This repository is currently the main repo for load forecasting. It contains:

- Training and evaluation logic.
- Inference logic intended to move to the edge side.
- Airflow DAGs on the central/host side.
- DVC and Git based snapshot tracking.

There is intentional duplication between the repo root and
`airflow_dags/include/forecasting/`.

Current interpretation:

- Repo root is the main development surface.
- `airflow_dags/include/forecasting/` is intended to become the deployable
  edge-side bundle later.

Long term goal:

- Edge performs inference, evaluation, drift detection, and data publication.
- Central Airflow performs orchestration for retraining.

## High-Level Architecture

### Edge Side Responsibilities

- Run inference on recent data.
- Evaluate production performance when actuals are available.
- Run data drift detection.
- Publish data and drift artifacts to DagsHub/DVC/Git.

### Central / Host Responsibilities

- Monitor snapshot SHA changes.
- Detect whether new data or drift artifacts require retraining.
- Trigger retraining workflows.
- Keep retraining logic separate from edge inference runtime.

## Retraining Trigger Design

Two independent snapshot channels are planned:

- `data-snapshots`
  - Used for new raw or processed edge data snapshots.
  - Central Airflow can react to SHA changes here.

- `drift-snapshots`
  - Used for JSON artifacts containing drift and retraining trigger metadata.
  - Central Airflow can react to SHA changes here independently.

This separation is intentional:

- New data arrival is not the same as model health degradation.
- Drift-based retraining should be decoupled from pure data availability.

## Drift Detection Design

Data drift detection will run on the edge side.

The current agreed approach uses Wasserstein distance and two reference
comparisons.

### 1. Static Drift

Compare:

- Static reference data
- Current production data

Static reference data is expected to be based on training data.

Purpose:

- Detect long-term distribution change relative to the data the model was
  originally trained on.
- Capture slow regime shifts and persistent changes in operating conditions.

Interpretation:

- High static drift can be expected over time and is not automatically an
  anomaly.
- It becomes meaningful when it remains elevated or is supported by
  performance degradation.

### 2. Recent Drift

Compare:

- Recent reference data
- Current production data

Recent reference data is planned as a rolling 7-day window of prior data.

Purpose:

- Detect sudden changes or anomalies relative to the system's recent behavior.
- Capture abrupt operational shifts that may not appear clearly against the
  original training distribution.

Interpretation:

- High recent drift is a short-term warning signal.
- It should not trigger retraining immediately unless persistent or supported
  by performance degradation.

## Why Two References Are Needed

A single reference does not answer both of these questions well:

- "Has the live data moved away from training-time behavior?"
- "Has something changed abruptly compared with recent normal behavior?"

Using both references separates:

- Long-term regime shift
- Short-term anomaly detection

## Role of Performance Degradation

Performance degradation is the primary retraining signal.

Agreed principle:

- If production error increases materially, that is the strongest signal for
  retraining.
- Data drift alone is not the primary trigger.
- Data drift should support decisions, provide early warning, and trigger only
  when strong and persistent.

Current intended decision hierarchy:

- Performance drift high:
  - Primary retraining trigger.

- Data drift high and persistent:
  - Secondary retraining trigger.

- Data drift high but temporary:
  - Warning and monitoring signal, not immediate retraining.

## Expected Combined Logic

The intended interpretation is:

- High performance degradation:
  - Retrain.

- High static drift, low recent drift:
  - Likely long-term distribution evolution.
  - Monitor closely, retrain if persistent or if performance degrades.

- High recent drift, low static drift:
  - Likely sudden anomaly or abrupt change.
  - Monitor for persistence and validate data quality.

- High static drift and high recent drift:
  - Stronger non-performance evidence for retraining.

- High drift plus high production error:
  - Strongest overall retraining case.

## Important Implementation Constraints

To keep drift results meaningful:

- Static reference data should be versioned with the model or training dataset.
- Recent reference data should be produced using the same preprocessing as
  production data.
- Production data should be compared in the same feature space used by the
  model.
- Thresholds for static drift and recent drift should be different.
- Persistence logic should be part of trigger evaluation.

## Pending Implementation Details

The following still need to be finalized:

- Exact file formats for:
  - Static reference data
  - Recent reference data
  - Production data
- Exact feature columns used for Wasserstein distance.
- Aggregation method across features.
- Definition of "persistent" drift.
- JSON schema for artifacts pushed to `drift-snapshots`.
- Airflow logic for merging:
  - new data signals
  - data drift signals
  - performance drift signals

## Current Direction for Drift Artifacts

The planned `drift-snapshots` branch should store JSON artifacts that describe:

- Timestamp
- Model version or model SHA
- Data window used
- Static drift metrics
- Recent drift metrics
- Performance metrics
- Thresholds used
- Trigger decision
- Reason for trigger

This allows central Airflow to react to drift branch changes without mixing
that logic with raw data snapshot updates.

## Summary

Current agreed design:

- Edge owns inference, evaluation, and drift detection.
- Central Airflow owns retraining orchestration.
- Performance degradation is the primary retraining signal.
- Data drift is a secondary but important signal.
- Two reference baselines will be used:
  - static training reference
  - recent 7-day reference
- Drift artifacts will be published separately through `drift-snapshots`.

This document should be updated as soon as the file schemas, thresholds, and
trigger policy are finalized.

## Current Implementation Notes

The current drift implementation work is happening in:

- `airflow_dags/include/forecasting/src/drift_detection/data_drift_detection.ipynb`
- `airflow_dags/include/forecasting/src/drift_detection/data_drift_detection.py`

Current understanding:

- The Python module and notebook are being kept in sync.
- The Python module is now the clearer operational reference.

## Current Data Schema

The current `inference.yaml` drift schema uses these columns:

- `FeatureType`
- `SiteID`
- `Type`
- `SeasonBin`
- `HourBin`
- `DayType`
- `IsHoliday`
- `Value`

Current `data_drift` config shape:

- `reference_training_data`
- `reference_recent_data`
- `production_data`
- `drift_features`
- `columns`
- `output_file`

Current configured drift features:

- `temperature`
- `netload_lag1`
- `netload_lag2`
- `netload_lag3`
- `netload_lag4`
- `netload_lag5`

Important current decision:

- Column names are currently hardcoded in the drift logic instead of being
  dynamically mapped from the YAML list.

Hardcoded column constants currently assumed in the implementation:

- `FEATURE_COL = "FeatureType"`
- `SEASON_COL = "SeasonBin"`
- `HOUR_COL = "HourBin"`
- `DAYTYPE_COL = "DayType"`
- `HOLIDAY_COL = "IsHoliday"`
- `VALUE_COL = "Value"`

## Current Grouping Logic

Current feature handling is split into two groups:

- Temperature-like features
- Lag features

### Temperature Grouping

Temperature is currently identified using case-insensitive matching on
`FeatureType` containing `temperature`.

Temperature drift is computed by grouping on:

- `SeasonBin`
- `HourBin`

Interpretation:

- Production temperature values are only compared with reference temperature
  values from the same season bin and hour bin.

### Lag Feature Grouping

Lag features are currently identified using case-insensitive matching on
`FeatureType` containing `lag`.

Lag drift is computed by grouping on:

- `DayType`
- `IsHoliday`
- `HourBin`

Interpretation:

- Production lag values are compared with reference lag values from the same
  lag feature name and the same day-type / holiday / hour bucket.

## Current Implemented Functions

The current implementation includes these functions:

- `group_features(reference_data, production_data)`
  - Separates temperature rows and lag rows from both reference and production
    datasets.

- `group_lag_features(reference_data, production_data, lag_val)`
  - Filters both datasets for one specific lag feature.

- `static_reference_drift(static_data, production_data)`
  - Validates input columns.
  - Validates that production features belong to the allowed feature list.
  - Computes grouped Wasserstein drift for temperature and lag features.
  - Uses production-driven grouped comparison.
  - Uses fallback when exact reference bins are absent or too small.
  - Computes normalized drift where IQR fallback succeeds.
  - Stores fallback provenance for each scored bin.
  - Stores results under `drift_scores["static"]`.

- `recent_reference_drift(recent_data, production_data)`
  - Still not implemented beyond validation scaffolding.

- `detect_drift_wasserstein(reference_data, production_data)`
  - Extracts `Value` arrays.
  - Returns `None` if either side is empty.
  - Returns `None` if all values are null after `dropna()`.
  - Computes Wasserstein distance and returns a scalar drift score.

- `fallback_reference_absent(reference_data, feature_name, bin_prod)`
  - Temperature drift fallback ladder.
  - Returns both the selected reference slice and the fallback level label.

- `fallback_reference_absent_lag(reference_data, lag_val, bin_prod)`
  - Lag drift fallback ladder.
  - Returns both the selected reference slice and the fallback level label.

- `fallback_reference_for_iqr_temperature(reference_data, feature_name, bin_prod)`
  - Temperature IQR fallback ladder.
  - Returns the first slice with enough samples and non-zero IQR.

- `fallback_reference_for_iqr_lag(reference_data, lag_val, bin_prod)`
  - Lag IQR fallback ladder.
  - Returns the first slice with enough samples and non-zero IQR.

## Current Result Shape

Current static drift result shape is:

- `drift_scores["static"]["temperature"]`
  - keyed by `(SeasonBin, HourBin)`

- `drift_scores["static"]["temperature_normalized"]`
  - keyed by `(SeasonBin, HourBin)`

- `drift_scores["static"]["bin_used_for_temperature_drift"]`
  - keyed by `(SeasonBin, HourBin)`
  - value is the comparison level used

- `drift_scores["static"]["lag"]`
  - keyed by `(lag_feature, DayType, IsHoliday, HourBin)`

- `drift_scores["static"]["lag_normalized"]`
  - keyed by `(lag_feature, DayType, IsHoliday, HourBin)`

- `drift_scores["static"]["bin_used_for_lag_drift"]`
  - keyed by `(lag_feature, DayType, IsHoliday, HourBin)`
  - value is the comparison level used

Example structure:

```python
{
    "static": {
        "temperature": {
            ("Winter", "Bin1"): 0.0
        },
        "temperature_normalized": {
            ("Winter", "Bin1"): 0.4
        },
        "bin_used_for_temperature_drift": {
            ("Winter", "Bin1"): "exact_bin"
        },
        "lag": {
            ("netload_lag2", "Weekday", "No", "Bin2"): 1.08
        },
        "lag_normalized": {
            ("netload_lag2", "Weekday", "No", "Bin2"): 0.91
        },
        "bin_used_for_lag_drift": {
            ("netload_lag2", "Weekday", "No", "Bin2"): "full_feature"
        }
    }
}
```

Reason for the lag key shape:

- Lag bins alone are not enough because different lag features can share the
  same `(DayType, IsHoliday, HourBin)` combination.
- The lag feature name is included in the key to avoid overwriting results.

## Current Comparison Provenance

Fallback provenance is now stored directly as a string in the per-bin
`bin_used_*` dictionaries.

Current temperature provenance values:

- `exact_bin`
- `season_only`
- `hour_only`
- `full_feature`

Current lag provenance values:

- `exact_bin`
- `day_hour`
- `hour_only`
- `full_feature`

This is intentionally simpler than storing both:

- whether fallback was used
- which fallback level was used

Only the final level used is stored now.

## Important Python Detail Used

The implementation currently uses tuple unpacking like:

```python
(lag_val, *bin_ref)
```

If:

```python
bin_ref = ("Weekday", "No", "Bin2")
lag_val = "netload_lag2"
```

then:

```python
(lag_val, *bin_ref)
```

becomes:

```python
("netload_lag2", "Weekday", "No", "Bin2")
```

This is used to build a flat tuple key rather than a nested tuple like:

```python
("netload_lag2", ("Weekday", "No", "Bin2"))
```

## Corrections Already Applied

The following issues were already fixed during implementation:

- Column-name mismatch between `Season_Bin` and `SeasonBin`
- Consistent use of `HourBin`
- Empty reference or production group guard
- Empty value-array guard after `dropna()`
- Lag-result key collision fix by including lag feature name in the key
- Returning scalar static drift values instead of nested single-item dicts
- Lowercase feature compatibility with YAML drift feature names
- Drift fallback now handles both:
  - exact bin absent
  - exact bin present but too small
- IQR fallback now handles both:
  - too few samples
  - zero or unusable IQR
- Temperature and lag normalization now use IQR fallback helpers
- Provenance is now stored per bin using a single descriptive string value
- Config path resolution in the Python module now points to the correct
  `inference.yaml`

## Current Known Limitations

These items are still intentionally unresolved:

- `recent_reference_drift` is still a placeholder.
- Drift normalization is not currently the focus and is intentionally being
  ignored for now.
- `check_consecutive_drifts` has not yet been aligned with the current nested
  output structure and is being ignored for now.
- `SiteID` and `Type` are present in schema but not used in the current drift
  logic because the current assumption is site-specific files.
- The current script does not yet store timestamps with drift outputs.
- There is still no minimum production sample rule.
- The current script does not yet persist drift history across runs.

## Current Fallback Logic

### Temperature Drift Fallback

Current temperature drift fallback ladder:

1. `SeasonBin + HourBin`
2. `SeasonBin`
3. `HourBin`
4. full `temperature` reference

Exact bin is considered unusable if:

- it is absent, or
- it exists but has fewer than `min_ref_samples_for_drift`

### Lag Drift Fallback

Current lag drift fallback ladder:

1. `DayType + IsHoliday + HourBin`
2. `DayType + HourBin`
3. `HourBin`
4. full lag-feature reference

Exact lag bin is considered unusable if:

- it is absent, or
- it exists but has fewer than `min_ref_samples_for_drift`

### Temperature IQR Fallback

Current temperature IQR fallback ladder uses the same order as temperature
drift fallback, but stops only when:

- candidate has at least `min_ref_samples_for_iqr`, and
- candidate IQR is non-zero

### Lag IQR Fallback

Current lag IQR fallback ladder uses the same order as lag drift fallback, but
stops only when:

- candidate has at least `min_ref_samples_for_iqr`, and
- candidate IQR is non-zero

## Current Design Decision

Current agreed architectural decision:

- `data_drift_detection.py` should remain focused on drift computation only.
- It should not directly decide investigation or retraining.
- Trigger logic should be implemented elsewhere:
  - another script,
  - a persistence layer / database,
  - or the Airflow + DVC pipeline / `drift-snapshots` path.

Reason:

- keep calculation deterministic and testable
- keep policy and orchestration separate
- make it easier to combine drift with performance degradation later

## Planned Work Later

- Add recent-reference drift logic
- Add timestamp fields to drift outputs
- Add minimum production sample logic
- Add persistence/history mechanism for drift outputs
- Add separate trigger policy layer for:
  - investigate vs retrain
  - persistence of drift across cycles
  - combination with performance degradation

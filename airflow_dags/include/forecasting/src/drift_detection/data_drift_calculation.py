import os
from pathlib import Path
import pandas as pd
import yaml
from scipy.stats import wasserstein_distance


config_path = Path(__file__).resolve()
infer_path = infer_path = config_path.parent.parent.parent / "inference.yaml"
with open(infer_path, "r") as file:
    config = yaml.safe_load(file)

features = config["data_drift"]["drift_features"]
min_ref_samples_for_iqr = config["data_drift"]["min_ref_samples_for_iqr"]
min_ref_samples_for_drift = config["data_drift"]["min_ref_samples_for_drift"]

drift_scores = {}
FEATURE_COL = "FeatureType"
SEASON_COL = "SeasonBin"
HOUR_COL = "HourBin"
DAYTYPE_COL = "DayType"
HOLIDAY_COL = "IsHoliday"
VALUE_COL = "Value"


def group_features(reference_data, production_data):
    temp_df_production = production_data[
        production_data[FEATURE_COL].str.contains("temperature", case=False, na=False)
    ]
    lag_df_production = production_data[
        production_data[FEATURE_COL].str.contains("lag", case=False, na=False)
    ]
    temp_df_reference = reference_data[
        reference_data[FEATURE_COL].str.contains("temperature", case=False, na=False)
    ]
    lag_df_reference = reference_data[
        reference_data[FEATURE_COL].str.contains("lag", case=False, na=False)
    ]
    return temp_df_reference, lag_df_reference, temp_df_production, lag_df_production


def group_lag_features(reference_data, production_data, lag_val):
    lag_df_reference = reference_data[reference_data[FEATURE_COL] == lag_val]
    lag_df_production = production_data[production_data[FEATURE_COL] == lag_val]
    return lag_df_reference, lag_df_production


def static_reference_drift(static_data, production_data):
    required_columns = {
        FEATURE_COL,
        SEASON_COL,
        HOUR_COL,
        DAYTYPE_COL,
        HOLIDAY_COL,
        VALUE_COL,
    }
    missing_static = required_columns - set(static_data.columns)
    missing_production = required_columns - set(production_data.columns)
    if missing_static:
        raise ValueError(f"Missing columns in static data: {missing_static}")
    if missing_production:
        raise ValueError(f"Missing columns in production data: {missing_production}")

    unknown = {
        feature
        for feature in production_data[FEATURE_COL].dropna().astype(str).unique()
        if feature.lower() not in set(features)
    }
    if unknown:
        raise ValueError(f"Unknown features in production data: {unknown}")

    (
        temp_df_reference,
        lag_df_reference,
        temp_df_production,
        lag_df_production,
    ) = group_features(static_data, production_data)

    static_temp_scores = {}
    normalized_static_temp_scores = {}
    static_lag_scores = {}
    normalized_static_lag_scores = {}

    temp_ref_groups = dict(tuple(temp_df_reference.groupby([SEASON_COL, HOUR_COL])))
    bin_used_temperature = {}
    level_used_temperature = {}
    for bin_prod, group_prod in temp_df_production.groupby([SEASON_COL, HOUR_COL]):
        group_ref = temp_ref_groups.get(bin_prod)
        
        bin_used_temp = "exact_match"
        level_used_temp = "exact_bin"
        if group_ref is None or len(group_ref) < min_ref_samples_for_drift:
            group_ref, level = fallback_reference_absent(temp_df_reference, "temperature", bin_prod)
            bin_used_temp = "fallback"
            level_used = level
        if group_ref is None:
            continue

        static_temp_score = detect_drift_wasserstein(group_ref, group_prod)
        if static_temp_score is not None:
            static_temp_scores[bin_prod] = static_temp_score
            bin_used_temperature[bin_prod] = bin_used_temp
            level_used_temperature[bin_prod] = level_used_temp
            iqr_ref = group_ref
            if len(iqr_ref) < min_ref_samples_for_iqr:
                iqr_ref = fallback_reference_for_iqr_temperature(
                    temp_df_reference, "temperature", bin_prod
                )

            if iqr_ref is not None:
                iqr_value = (iqr_ref[VALUE_COL].quantile(0.75)- iqr_ref[VALUE_COL].quantile(0.25))
                if iqr_value > 0:
                    normalized_static_temp_scores[bin_prod] = static_temp_score / iqr_value

    bin_used_lag = {}
    level_used_lag = {}
    for lag_val in lag_df_reference[FEATURE_COL].dropna().unique():
        lag_ref, lag_prod = group_lag_features(lag_df_reference, lag_df_production, lag_val)
        lag_ref_groups = dict(tuple(lag_ref.groupby([DAYTYPE_COL, HOLIDAY_COL, HOUR_COL])))
        for bin_prod, group_prod in lag_prod.groupby([DAYTYPE_COL, HOLIDAY_COL, HOUR_COL]):
            group_ref = lag_ref_groups.get(bin_prod)
            bin_used_lag_key = "exact_match"
            level_used_lag_key = "exact_bin"
            if group_ref is None or len(group_ref) < min_ref_samples_for_drift:
                group_ref, level = fallback_reference_absent_lag(lag_ref, lag_val, bin_prod)
                bin_used_lag_key = "fallback"
                level_used_lag_key = level
            if group_ref is None:
                continue

            static_lag_score = detect_drift_wasserstein(group_ref, group_prod)
            if static_lag_score is not None:
                lag_key = (lag_val, *bin_prod)
                static_lag_scores[lag_key] = static_lag_score
                bin_used_lag[lag_key] = bin_used_lag_key
                level_used_lag[lag_key] = level_used_lag_key
                iqr_ref = group_ref
                if len(iqr_ref) < min_ref_samples_for_iqr:
                    iqr_ref = fallback_reference_for_iqr_lag(lag_ref, lag_val, bin_prod)

                if iqr_ref is not None:
                    iqr_value = (iqr_ref[VALUE_COL].quantile(0.75)- iqr_ref[VALUE_COL].quantile(0.25))
                    if iqr_value > 0:
                        normalized_static_lag_scores[lag_key] = static_lag_score / iqr_value

    drift_scores["static"] = {
        "temperature": static_temp_scores,
        "temperature_normalized": normalized_static_temp_scores,
        "bin_used_for_temperature_drift": bin_used_temperature,
        "level_used_for_temperature_drift": level_used_temperature,
        "lag": static_lag_scores,
        "lag_normalized": normalized_static_lag_scores,
        "bin_used_for_lag_drift": bin_used_lag,
        "level_used_for_lag_drift": level_used_lag,
    }
    print(drift_scores)
    return drift_scores


def fallback_reference_absent(reference_data, feature_name, bin_prod):
    season_bin, hour_bin = bin_prod
    feature_ref = reference_data[reference_data[FEATURE_COL] == feature_name]

    candidates = [(
        feature_ref[
            (feature_ref[SEASON_COL] == season_bin)
            & (feature_ref[HOUR_COL] == hour_bin)
        ], "exact_bin"),
        (
        feature_ref[feature_ref[SEASON_COL] == season_bin], "season_only"),
        (
        feature_ref[feature_ref[HOUR_COL] == hour_bin], "hour_only"),
        (feature_ref, "full_feature")
    ]

    for candidate, level in candidates:
        if (
            candidate is not None
            and not candidate.empty
            and len(candidate) >= min_ref_samples_for_drift
        ):
            return candidate, level

    return None


def fallback_reference_absent_lag(reference_data, lag_val, bin_prod):
    day_type_bin, is_holiday_bin, hour_bin = bin_prod
    feature_ref = reference_data[reference_data[FEATURE_COL] == lag_val]

    candidates = [
        feature_ref[
            (feature_ref[DAYTYPE_COL] == day_type_bin)
            & (feature_ref[HOLIDAY_COL] == is_holiday_bin)
            & (feature_ref[HOUR_COL] == hour_bin)
        ],
        feature_ref[
            (feature_ref[DAYTYPE_COL] == day_type_bin)
            & (feature_ref[HOUR_COL] == hour_bin)
        ],
        feature_ref[feature_ref[HOUR_COL] == hour_bin],
        feature_ref,
    ]

    for candidate in candidates:
        if (
            candidate is not None and not candidate.empty and len(candidate) >= min_ref_samples_for_drift
        ):
            return candidate

    return None


def recent_reference_drift(recent_data, production_data):
    required_columns = {
        FEATURE_COL,
        SEASON_COL,
        HOUR_COL,
        DAYTYPE_COL,
        HOLIDAY_COL,
        VALUE_COL,
    }
    missing_static = required_columns - set(recent_data.columns)
    missing_production = required_columns - set(production_data.columns)
    if missing_static:
        raise ValueError(f"Missing columns in static data: {missing_static}")
    if missing_production:
        raise ValueError(f"Missing columns in production data: {missing_production}")

    unknown = {
        feature
        for feature in production_data[FEATURE_COL].dropna().astype(str).unique()
        if feature.lower() not in set(features)
    }
    if unknown:
        raise ValueError(f"Unknown features in production data: {unknown}")


def fallback_reference_for_iqr_temperature(reference_data, feature_name, bin_prod):
    season_bin, hour_bin = bin_prod
    feature_ref = reference_data[reference_data[FEATURE_COL] == feature_name]

    candidates = [
        feature_ref[
            (feature_ref[SEASON_COL] == season_bin)
            & (feature_ref[HOUR_COL] == hour_bin)
        ],
        feature_ref[feature_ref[SEASON_COL] == season_bin],
        feature_ref[feature_ref[HOUR_COL] == hour_bin],
        feature_ref,
    ]

    for candidate in candidates:
        if candidate is None or candidate.empty or len(candidate) < min_ref_samples_for_iqr:
            continue

        iqr_value = candidate[VALUE_COL].quantile(0.75) - candidate[VALUE_COL].quantile(0.25)
        if iqr_value > 0:
            return candidate

    return None


def fallback_reference_for_iqr_lag(reference_data, lag_val, bin_prod):
    day_type_bin, is_holiday_bin, hour_bin = bin_prod
    feature_ref = reference_data[reference_data[FEATURE_COL] == lag_val]

    candidates = [
        feature_ref[
            (feature_ref[DAYTYPE_COL] == day_type_bin)
            & (feature_ref[HOLIDAY_COL] == is_holiday_bin)
            & (feature_ref[HOUR_COL] == hour_bin)
        ],
        feature_ref[
            (feature_ref[DAYTYPE_COL] == day_type_bin)
            & (feature_ref[HOUR_COL] == hour_bin)
        ],
        feature_ref[feature_ref[HOUR_COL] == hour_bin],
        feature_ref,
    ]

    for candidate in candidates:
        if candidate is None or candidate.empty or len(candidate) < min_ref_samples_for_iqr:
            continue

        iqr_value = candidate[VALUE_COL].quantile(0.75) - candidate[VALUE_COL].quantile(0.25)
        if iqr_value > 0:
            return candidate

    return None


def detect_drift_wasserstein(reference_data, production_data):
    if reference_data.empty or production_data.empty:
        return None

    reference_values = reference_data[VALUE_COL].dropna().values
    production_values = production_data[VALUE_COL].dropna().values
    if len(reference_values) == 0 or len(production_values) == 0:
        return None

    return wasserstein_distance(reference_values, production_values)


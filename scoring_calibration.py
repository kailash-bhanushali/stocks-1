from datetime import datetime, timezone
import math
import statistics


class CalibrationError(RuntimeError):
    pass


def _pct_change(current, previous):
    if previous in (None, 0) or current is None:
        return None
    return ((current - previous) / abs(previous)) * 100


def _average(values):
    clean = [float(value) for value in values if value is not None]
    return sum(clean) / len(clean) if clean else None


def _rsi(closes, period):
    if len(closes) < period + 1:
        return None
    gains = []
    losses = []
    for previous, current in zip(closes[-period - 1:-1], closes[-period:]):
        change = current - previous
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))
    avg_gain = _average(gains) or 0
    avg_loss = _average(losses) or 0
    if avg_loss == 0:
        return 100 if avg_gain > 0 else 50
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def build_observations(section, bars_by_symbol, config, target_days=5):
    observations = []
    for symbol, bars in bars_by_symbol.items():
        ordered = sorted(bars, key=lambda row: row.get("date") or "")
        closes = [float(row["close"]) for row in ordered]
        volumes = [float(row.get("volume") or 0) for row in ordered]
        start = 21 if section == "discovery" else max(61, config["sma_long_days"], config["rsi_days"] + 1)
        for index in range(start, len(ordered) - target_days, target_days):
            close = closes[index]
            future = _pct_change(closes[index + target_days], close)
            if future is None:
                continue
            r5 = _pct_change(close, closes[index - 5])
            r20 = _pct_change(close, closes[index - 20])
            average_volume = _average(volumes[index - 20:index])
            volume_ratio = volumes[index] / average_volume if average_volume else 1
            if section == "discovery":
                features = {
                    "return_5d": r5 or 0,
                    "return_20d": r20 or 0,
                    "volume_expansion": max(0, volume_ratio - 1),
                }
            else:
                r60 = _pct_change(close, closes[index - 60])
                short_sma = _average(closes[index - config["sma_short_days"] + 1:index + 1])
                long_sma = _average(closes[index - config["sma_long_days"] + 1:index + 1])
                rsi_value = _rsi(closes[:index + 1], config["rsi_days"])
                features = {
                    "return_5d": r5 or 0,
                    "return_20d": r20 or 0,
                    "return_60d": r60 or 0,
                    "above_short_sma": 1 if short_sma and close > short_sma else 0,
                    "above_long_sma": 1 if long_sma and close > long_sma else 0,
                    "volume_expansion": max(0, volume_ratio - config["minimum_volume_ratio"]),
                    "overbought": 1 if rsi_value and rsi_value > config["overbought_rsi"] else 0,
                }
            observations.append({
                "date": ordered[index].get("date"),
                "symbol": symbol,
                "features": features,
                "target": future,
            })
    return observations


def _solve_linear(matrix, vector):
    size = len(vector)
    augmented = [list(map(float, matrix[row])) + [float(vector[row])] for row in range(size)]
    for column in range(size):
        pivot = max(range(column, size), key=lambda row: abs(augmented[row][column]))
        if abs(augmented[pivot][column]) < 1e-12:
            raise CalibrationError("Calibration matrix is singular")
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        divisor = augmented[column][column]
        augmented[column] = [value / divisor for value in augmented[column]]
        for row in range(size):
            if row == column:
                continue
            factor = augmented[row][column]
            augmented[row] = [
                augmented[row][item] - factor * augmented[column][item]
                for item in range(size + 1)
            ]
    return [augmented[row][-1] for row in range(size)]


def _fit_ridge(observations, feature_names, ridge=5.0):
    means = {}
    deviations = {}
    for name in feature_names:
        values = [row["features"][name] for row in observations]
        means[name] = statistics.fmean(values)
        deviations[name] = statistics.pstdev(values) or 1.0
    rows = []
    targets = []
    for observation in observations:
        rows.append([1.0] + [
            (observation["features"][name] - means[name]) / deviations[name]
            for name in feature_names
        ])
        targets.append(observation["target"])
    width = len(feature_names) + 1
    xtx = [[0.0] * width for _ in range(width)]
    xty = [0.0] * width
    for row, target in zip(rows, targets):
        for left in range(width):
            xty[left] += row[left] * target
            for right in range(width):
                xtx[left][right] += row[left] * row[right]
    for index in range(1, width):
        xtx[index][index] += ridge
    coefficients = _solve_linear(xtx, xty)
    return coefficients, means, deviations


def _predict(observation, feature_names, coefficients, means, deviations):
    prediction = coefficients[0]
    for index, name in enumerate(feature_names, start=1):
        prediction += coefficients[index] * (
            (observation["features"][name] - means[name]) / deviations[name]
        )
    return prediction


def _correlation(left, right):
    if len(left) < 2:
        return 0.0
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    numerator = sum((x - left_mean) * (y - right_mean) for x, y in zip(left, right))
    left_ss = sum((x - left_mean) ** 2 for x in left)
    right_ss = sum((y - right_mean) ** 2 for y in right)
    denominator = math.sqrt(left_ss * right_ss)
    return numerator / denominator if denominator else 0.0


def calibrate(section, bars_by_symbol, config, target_days=5, ridge=5.0):
    if section not in ("discovery", "market"):
        raise CalibrationError("Historical calibration is supported for discovery and market scoring")
    observations = build_observations(section, bars_by_symbol, config, target_days)
    dates = sorted({row["date"] for row in observations if row.get("date")})
    if len(dates) < 40 or len(observations) < 160:
        raise CalibrationError("Not enough point-in-time observations; use more symbols or a longer lookback")
    split_date = dates[max(1, int(len(dates) * 0.70)) - 1]
    training = [row for row in observations if row["date"] <= split_date]
    validation = [row for row in observations if row["date"] > split_date]
    if len(training) < 100 or len(validation) < 40:
        raise CalibrationError("Not enough training or validation observations")
    feature_names = list(training[0]["features"])
    coefficients, means, deviations = _fit_ridge(training, feature_names, ridge)
    training_predictions = [
        _predict(row, feature_names, coefficients, means, deviations) for row in training
    ]
    validation_predictions = [
        _predict(row, feature_names, coefficients, means, deviations) for row in validation
    ]
    validation_targets = [row["target"] for row in validation]
    prediction_std = statistics.pstdev(training_predictions) or 1.0
    points_scale = 10.0 / prediction_std
    raw_weights = {
        name: (coefficients[index] / deviations[name]) * points_scale
        for index, name in enumerate(feature_names, start=1)
    }
    ranked = sorted(zip(validation_predictions, validation_targets), key=lambda row: row[0], reverse=True)
    top_count = max(1, len(ranked) // 4)
    top_targets = [target for _, target in ranked[:top_count]]
    hit_rate = sum(1 for predicted, target in zip(validation_predictions, validation_targets) if (predicted >= 0) == (target >= 0)) / len(validation_targets)
    validation_correlation = _correlation(validation_predictions, validation_targets)
    correlation_t = validation_correlation * math.sqrt(
        (len(validation_targets) - 2) / max(1e-12, 1 - validation_correlation ** 2)
    )
    metrics = {
        "validation_correlation": round(validation_correlation, 4),
        "validation_correlation_t_stat": round(correlation_t, 4),
        "validation_directional_accuracy": round(hit_rate, 4),
        "validation_top_quartile_mean_return_pct": round(statistics.fmean(top_targets), 4),
        "validation_all_mean_return_pct": round(statistics.fmean(validation_targets), 4),
    }
    return {
        "status": "calibrated",
        "method": "70/30 chronological holdout with ridge regression",
        "target": f"forward_{target_days}_trading_day_return_pct",
        "sample_spacing_days": target_days,
        "symbols": sorted(bars_by_symbol),
        "training_observations": len(training),
        "validation_observations": len(validation),
        "training_start": min(row["date"] for row in training),
        "training_end": max(row["date"] for row in training),
        "validation_start": min(row["date"] for row in validation),
        "validation_end": max(row["date"] for row in validation),
        "ridge_penalty": ridge,
        "score_scaling": "one standard deviation of fitted return equals 10 score points",
        "weights": {name: round(value, 6) for name, value in raw_weights.items()},
        "metrics": metrics,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "limitations": [
            "Historical association is not a guarantee of future performance.",
            "Validation uses configured symbols and does not remove every survivorship or regime bias.",
            "Transaction costs and option execution are outside this coefficient fit.",
        ],
    }

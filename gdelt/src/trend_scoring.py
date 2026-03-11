import json
import math
from typing import Dict, Tuple


def parse_weights(weights: str) -> Dict[str, float]:
    defaults = {"z": 1.0, "g": 0.8, "v": 0.6, "n": 0.4}
    if not weights:
        return defaults
    parsed = {}
    for part in weights.split(","):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        parsed[key.strip()] = float(value.strip())
    return {**defaults, **parsed}


def volume_confidence(total_count: int, min_total: int) -> float:
    if total_count <= 0:
        return 0.0
    return min(1.0, math.log1p(total_count) / math.log1p(max(min_total, 1) + 1))


def novelty_bonus(novelty_days: float) -> float:
    if novelty_days < 0:
        return 0.0
    if novelty_days >= 30:
        return 0.0
    return (30 - novelty_days) / 30


def score_phrase(
    features: Dict,
    weights: Dict[str, float],
    min_total: int,
) -> Tuple[float, Dict[str, float]]:
    z_score = max(-5.0, min(5.0, float(features.get("z_score", 0.0))))
    current_count = float(features.get("current_count", 0.0))
    baseline_mean = float(features.get("baseline_mean", 0.0))
    growth = math.log1p(current_count) - math.log1p(baseline_mean)
    velocity = max(0.0, float(features.get("velocity", 0.0)))
    novelty = novelty_bonus(features.get("novelty_days", 0))

    base_score = (
        weights["z"] * z_score
        + weights["g"] * growth
        + weights["v"] * math.log1p(velocity)
        + weights["n"] * novelty
    )

    confidence = volume_confidence(int(current_count), min_total)
    return base_score * confidence, {"confidence": confidence}

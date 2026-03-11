from gdelt.src.trend_scoring import score_phrase, parse_weights


def test_scoring_monotonicity():
    weights = parse_weights("z=1.0,g=0.8,v=0.6,n=0.4")
    base_features = {
        "current_count": 5,
        "baseline_mean": 2.0,
        "baseline_std": 1.0,
        "z_score": 3.0,
        "growth_ratio": 3.0,
        "velocity": 2.0,
        "novelty_days": 5,
    }

    score_low, _ = score_phrase(base_features, weights, min_total=5)

    higher = dict(base_features)
    higher["current_count"] = 10
    higher["z_score"] = 8.0
    higher["growth_ratio"] = 5.0
    higher["velocity"] = 7.0

    score_high, _ = score_phrase(higher, weights, min_total=5)

    assert score_high > score_low

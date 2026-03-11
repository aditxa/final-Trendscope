from gdelt.src.trend_features import canonicalize_phrase


def test_dedup_normalization():
    assert canonicalize_phrase("Chocolate Chip Cookies") == "chocolate chip cookie"
    assert canonicalize_phrase("Spicy Ramen Recipe") == "spicy ramen"

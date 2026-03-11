from gdelt.src.extract_phrases import extract_candidates


def test_phrase_extraction_filters_generic():
    titles = [("Viral recipe for spicy ramen", "20250101000000")]
    counts = extract_candidates(titles)

    totals = sum(sum(day.values()) for day in counts.values())
    assert totals >= 1
    assert "viral recipe" not in next(iter(counts.values()))

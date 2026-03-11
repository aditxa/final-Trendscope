from gdelt.src.main import _is_food_trend_title


def test_title_filter_rejects_demonym_only_false_positive():
    assert _is_food_trend_title("Viral Chinese Trump wins laughs on both sides of Pacific") is False


def test_title_filter_accepts_cuisine_with_food_context():
    assert _is_food_trend_title("Viral Chinese cucumber salad recipe is trending on TikTok") is True


def test_title_filter_accepts_brand_promo_food_story():
    assert (
        _is_food_trend_title("McDonald offering a free McNugget Caviar kit: here how to get it")
        is True
    )

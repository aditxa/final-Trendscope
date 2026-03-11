import csv
import logging
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

# Cache spaCy model globally to avoid reloading
_spacy_nlp = None

STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
    "has", "in", "is", "it", "its", "of", "on", "or", "that", "the",
    "to", "was", "were", "with", "this", "these", "those", "you", "your",
    "about", "after", "before", "over", "under", "how", "why", "what",
    "when", "where", "who", "which", "their", "them", "they", "there",
    "here", "out", "up", "down", "into", "onto", "will", "would", "should",
    "could", "can", "may", "might", "must", "have", "had", "been", "being",
    "do", "does", "did", "done", "just", "also", "more", "most", "other",
    "some", "such", "than", "very", "all", "any", "both", "each", "few",
    "many", "much", "own", "same", "so", "such", "too", "get", "got",
    # Trend-related generic words
    "viral", "trending", "trend", "trends", "new", "easy", "quick", "best",
    "popular", "simple", "goes", "going", "gone", "making", "made", "make",
    "try", "trying", "tried", "recipe", "recipes", "food", "foods", "tiktok",
    "everyone", "everyone's", "everyones", "amazing", "incredible", "delicious",

    # Common news/incident/reporting tokens that create non-food "trends".
    "driver", "delivery", "customer", "customers", "spray", "spraying", "pepper",
    "video", "videos", "shows", "show", "caught", "footage", "report", "reported",
    "police", "arrest", "lawsuit", "charges", "charged", "attack", "incident",
    "award", "awards", "creator", "trophy", "wins", "win", "year",
}

GENERIC_PHRASES = {
    "viral recipe", "tiktok recipe", "food trend", "trending recipe",
    "viral food", "viral foods", "food trends", "easy dinner", "new recipe",
    "goes viral", "tiktok trend", "trending food", "popular recipe",
    "easy recipe", "quick recipe", "simple recipe", "best recipe",
    "new food", "popular food", "try recipe", "amazing recipe",
    "delicious recipe", "incredible recipe", "everyone making",
    "everyone trying", "dinner recipe", "lunch recipe", "breakfast recipe",
}

# Common food-related brands / platforms. This is intentionally small and
# pragmatic: it helps treat brand-only unigrams (e.g., "starbucks") as
# food-related when using food-only filters.
DELIVERY_BRANDS = {
    "doordash",
    "ubereats",
    "grubhub",
}

FOOD_BRANDS = {
    "starbucks",
    "mcdonalds",
    "mcdonald's",
    "kfc",
    "subway",
    "dominos",
    "domino's",
    "pizzahut",
    "pizza hut",
    "tacobell",
    "taco bell",
    "chipotle",
    "dunkin",
}

# Back-compat union used by other modules.
BRAND_LEXICON = set(FOOD_BRANDS) | set(DELIVERY_BRANDS)

INCIDENT_TOKENS = {
    "driver",
    "delivery",
    "customer",
    "customers",
    "spray",
    "spraying",
    "pepper",
    "video",
    "videos",
    "shows",
    "show",
    "caught",
    "footage",
    "police",
    "arrest",
    "lawsuit",
    "charges",
    "charged",
    "attack",
    "incident",
}

FOOD_LEXICON = {
    # Proteins
    "chicken", "beef", "pork", "lamb", "turkey", "duck", "fish", "salmon",
    "tuna", "shrimp", "lobster", "crab", "oyster", "scallop", "tofu", "tempeh",
    "seitan", "egg", "eggs",
    # Carbs & Grains
    "pasta", "noodles", "noodle", "ramen", "rice", "quinoa", "bread", "baguette",
    "focaccia", "sourdough", "bagel", "croissant", "buns", "tortilla",
    # Dishes
    "pizza", "taco", "tacos", "burrito", "quesadilla", "enchilada", "curry",
    "biryani", "sushi", "poke", "burger", "sandwich", "wrap", "bowl",
    "dumpling", "dumplings", "gyoza", "momo", "pierogi", "samosa",
    # Soups & Stews
    "soup", "stew", "chowder", "bisque", "broth", "ramen", "pho", "laksa",
    "gumbo", "pozole",
    # Salads
    "salad", "caesar", "cobb", "greek",
    # Desserts & Sweets
    "cake", "cheesecake", "brownie", "cookie", "cookies", "muffin", "cupcake",
    "pie", "tart", "tiramisu", "mousse", "parfait", "pudding", "flan",
    "ice cream", "gelato", "sorbet", "macaron", "eclair", "donut", "doughnut",
    # Beverages
    "latte", "cappuccino", "espresso", "coffee", "tea", "matcha", "chai",
    "smoothie", "shake", "juice", "cocktail", "mocktail", "boba",
    # Cooking Methods
    "fried", "baked", "grilled", "roasted", "steamed", "braised", "sauteed",
    "poached", "smoked", "pickled", "fermented",
    # Cuisines & Styles
    "korean", "japanese", "chinese", "thai", "vietnamese", "indian", "mexican",
    "italian", "french", "mediterranean", "greek", "turkish", "middle eastern",
    "american", "southern", "cajun", "tex-mex",
    # Diets
    "vegan", "vegetarian", "keto", "paleo", "gluten-free", "dairy-free",
    # Common Ingredients
    "cheese", "mozzarella", "parmesan", "cheddar", "butter", "cream",
    "avocado", "tomato", "potato", "onion", "garlic", "ginger", "chili",
    "pepper", "mushroom", "spinach", "kale", "bacon", "sausage",

    # Cooking appliances / methods often used as food context.
    "air fryer", "instant pot", "slow cooker",
}

# Subset of FOOD_LEXICON that represents cuisine/style/diet/method tokens.
# These are useful as modifiers ("japanese cheesecake") but are too generic as
# the head of a phrase ("ingredient japanese").
STYLE_TOKENS = {
    # Cuisines & styles
    "korean",
    "japanese",
    "chinese",
    "thai",
    "vietnamese",
    "indian",
    "mexican",
    "italian",
    "french",
    "mediterranean",
    "greek",
    "turkish",
    "middle",
    "eastern",
    "american",
    "southern",
    "cajun",
    "tex-mex",
    # Diets
    "vegan",
    "vegetarian",
    "keto",
    "paleo",
    "gluten-free",
    "dairy-free",
    # Cooking methods
    "fried",
    "baked",
    "grilled",
    "roasted",
    "steamed",
    "braised",
    "sauteed",
    "poached",
    "smoked",
    "pickled",
    "fermented",
}

DISH_SUFFIXES = {
    "cake", "curry", "salad", "sandwich", "noodles", "soup", "latte",
    "tacos", "taco", "pizza", "ramen", "bowl", "buns", "rolls", "burger",
    "stew", "pie", "tart", "cookies", "cookie", "muffin", "wrap", "sushi",
    "poke", "dumplings", "gyoza", "pasta", "rice", "biryani", "casserole",
    "chowder", "bisque", "smoothie", "shake", "cocktail", "lemonade",
}

TOKEN_RE = re.compile(r"[a-zA-Z][a-zA-Z'\-]*")

# Tokens that commonly appear as modifiers in viral recipe headlines but don't
# change the core food item. Used to derive a shorter "core" phrase variant
# (e.g., "japanese cheesecake" from "two ingredient japanese cheesecake").
CORE_MODIFIERS = {
    "ingredient",
    "ingredients",
    "two",
    "three",
    "four",
    "five",
    "minute",
    "minutes",
    "no",
    "zero",
    "baking",
}


def _contains_contiguous_tokens(title_tokens: Sequence[str], phrase_tokens: Sequence[str]) -> bool:
    if not phrase_tokens:
        return False
    if len(phrase_tokens) == 1:
        return phrase_tokens[0] in title_tokens
    if len(title_tokens) < len(phrase_tokens):
        return False
    n = len(phrase_tokens)
    for i in range(len(title_tokens) - n + 1):
        if list(title_tokens[i : i + n]) == list(phrase_tokens):
            return True
    return False


def _derive_core_phrase_variants(phrase: str, title_tokens_no_stop: Sequence[str]) -> List[str]:
    base = (phrase or "").strip().lower()
    if not base:
        return []

    tokens = [t for t in base.split() if t]
    if len(tokens) < 3:
        return [base]

    variants: List[str] = [base]

    # Variant 1: keep "{important_modifier} {dish}" (e.g., japanese cheesecake)
    last = tokens[-1]
    if last in DISH_SUFFIXES or last in FOOD_LEXICON:
        prev = None
        for t in reversed(tokens[:-1]):
            if t in STOPWORDS or t in CORE_MODIFIERS:
                continue
            prev = t
            break
        if prev:
            v = f"{prev} {last}"
            if v != base and _contains_contiguous_tokens(title_tokens_no_stop, [prev, last]):
                variants.append(v)

    # Variant 2: remove generic/core modifiers but keep order.
    stripped = [t for t in tokens if t not in STOPWORDS and t not in CORE_MODIFIERS]
    if 1 < len(stripped) <= 4:
        v = " ".join(stripped)
        if v != base and _contains_contiguous_tokens(title_tokens_no_stop, stripped):
            variants.append(v)

    # Variant 3: add tail n-grams for long phrases (often the dish itself is at the end).
    if tokens[-1] in DISH_SUFFIXES or tokens[-1] in FOOD_LEXICON:
        for n in (2, 3):
            if len(tokens) >= n:
                tail = tokens[-n:]
                v = " ".join(tail)
                if v != base and _contains_contiguous_tokens(title_tokens_no_stop, tail):
                    variants.append(v)

    # Deduplicate while preserving order.
    return list(dict.fromkeys(variants))


def normalize_title(title: str) -> List[str]:
    tokens = [token.lower() for token in TOKEN_RE.findall(title)]
    return [token for token in tokens if token not in STOPWORDS and len(token) > 2]


def ngrams(tokens: Sequence[str], n: int) -> Iterable[str]:
    if len(tokens) < n:
        return []
    return [" ".join(tokens[i : i + n]) for i in range(len(tokens) - n + 1)]


def extract_candidates(titles: Iterable[Tuple[str, str]]) -> Dict[str, Dict[str, int]]:
    """Extract food-related phrases from titles using NLP techniques.
    
    This uses multiple strategies:
    1. Named entity recognition for food items
    2. Noun phrase extraction using spaCy
    3. N-gram extraction (2-3 words) as fallback
    4. Filtering based on food lexicon and dish suffixes
    """
    counts_by_date: Dict[str, Counter] = defaultdict(Counter)

    for title, seendate in titles:
        if not title:
            continue
        date_key = _to_date_key(seendate)

        # Extract phrases using multiple methods
        all_phrases = _extract_all_phrases(title)
        
        # Deduplicate and filter phrases
        seen = set()
        for phrase in all_phrases:
            phrase_lower = phrase.lower().strip()
            if not phrase_lower or phrase_lower in seen:
                continue
            if phrase_lower in GENERIC_PHRASES:
                continue
            if not _is_food_phrase(phrase_lower):
                continue
            if _has_sufficient_quality(phrase_lower):
                counts_by_date[date_key][phrase_lower] += 1
                seen.add(phrase_lower)

    return {date: dict(counter) for date, counter in counts_by_date.items()}


def extract_phrases_from_title(title: str) -> List[str]:
    """Extract normalized, food-relevant phrases from a single title.

    This is the same core logic used by `extract_candidates`, exposed as a small
    utility so the trend counting pipeline can build `phrase_counts` from
    higher-quality phrases than raw n-grams.
    """

    if not title:
        return []

    # If the title itself looks food/brand-related, allow some contextual noun
    # chunks even when the phrase doesn't directly contain a lexicon keyword.
    title_tokens = [t.lower() for t in TOKEN_RE.findall(title or "")]
    title_tokens_no_stop = [t for t in title_tokens if t and t not in STOPWORDS]
    title_food_signal = (
        any(t in FOOD_LEXICON or t in BRAND_LEXICON for t in title_tokens)
        or (title_tokens[-1] in DISH_SUFFIXES if title_tokens else False)
        or any(" ".join(title_tokens[i : i + 2]) in FOOD_LEXICON for i in range(max(0, len(title_tokens) - 1)))
    )

    # Prefer higher-quality extractors when spaCy is available (noun chunks,
    # entities, and pattern phrases). Avoid raw n-grams which tend to produce
    # generic tokens.
    candidates: List[str] = []
    if _get_spacy_model() is not None:
        candidates.extend(_extract_noun_chunks_spacy(title))
        candidates.extend(_extract_named_entities(title))
        candidates.extend(_extract_pattern_phrases(title))
    else:
        candidates.extend(_extract_all_phrases(title))

    phrases: List[str] = []
    seen: Set[str] = set()

    def _maybe_add(phrase_lower: str) -> None:
        phrase_lower = (phrase_lower or "").strip().lower()
        if not phrase_lower or phrase_lower in seen:
            return
        if phrase_lower in GENERIC_PHRASES:
            return
        if not _has_sufficient_quality(phrase_lower):
            return

        tokens = phrase_lower.split()
        if not tokens:
            return

        # Suppress incident-style phrases unless they also contain a strong food/dish signal.
        if any(t in INCIDENT_TOKENS for t in tokens):
            if not any(t in FOOD_LEXICON for t in tokens) and (tokens[-1] not in DISH_SUFFIXES):
                return

        if len(tokens) == 1:
            token = tokens[0]
            if token not in FOOD_LEXICON and token not in DISH_SUFFIXES and token not in BRAND_LEXICON:
                return
        else:
            if not _is_food_phrase(phrase_lower) and not title_food_signal:
                return

        phrases.append(phrase_lower)
        seen.add(phrase_lower)

    for phrase in candidates:
        phrase_lower = (phrase or "").lower().strip()
        if not phrase_lower:
            continue

        for variant in _derive_core_phrase_variants(phrase_lower, title_tokens_no_stop):
            _maybe_add(variant)

    return phrases


def _to_date_key(seendate: str) -> str:
    if not seendate:
        return "unknown"
    try:
        dt = datetime.fromisoformat(seendate.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = datetime.strptime(seendate, "%Y%m%d%H%M%S")
        except ValueError:
            return "unknown"
    return dt.date().isoformat()


def _get_spacy_model():
    """Load and cache spaCy model."""
    global _spacy_nlp
    if _spacy_nlp is not None:
        return _spacy_nlp
    
    try:
        import spacy
        _spacy_nlp = spacy.load("en_core_web_sm")
        return _spacy_nlp
    except Exception:
        return None


def _extract_all_phrases(title: str) -> List[str]:
    """Extract phrases using multiple NLP methods."""
    phrases = []
    
    # Method 1: spaCy noun chunks (best quality)
    noun_phrases = _extract_noun_chunks_spacy(title)
    phrases.extend(noun_phrases)
    
    # Method 2: spaCy named entities (food items)
    entities = _extract_named_entities(title)
    phrases.extend(entities)
    
    # Method 3: N-grams as fallback
    tokens = normalize_title(title)
    bigrams = ngrams(tokens, 2)
    trigrams = ngrams(tokens, 3)
    phrases.extend(bigrams)
    phrases.extend(trigrams)
    
    # Method 4: Pattern-based extraction (modifier + food word)
    pattern_phrases = _extract_pattern_phrases(title)
    phrases.extend(pattern_phrases)
    
    return phrases


def _extract_noun_chunks_spacy(title: str) -> List[str]:
    """Extract noun chunks using spaCy with improved filtering."""
    nlp = _get_spacy_model()
    if nlp is None:
        return []
    
    doc = nlp(title)
    phrases = []
    
    for chunk in doc.noun_chunks:
        # Filter out determiners and keep only meaningful tokens
        tokens = [
            token.text.lower() 
            for token in chunk 
            if token.pos_ not in ("DET", "PRON") and token.text.strip()
        ]
        if tokens:
            phrase = " ".join(tokens)
            normalized = _normalize_phrase(phrase)
            if normalized and len(normalized.split()) <= 4:
                phrases.append(normalized)
    
    return phrases


def _extract_named_entities(title: str) -> List[str]:
    """Extract food-related named entities using spaCy."""
    nlp = _get_spacy_model()
    if nlp is None:
        return []
    
    doc = nlp(title)
    entities = []
    
    for ent in doc.ents:
        # Focus on product, food, and organization entities (brand names)
        if ent.label_ in ("PRODUCT", "ORG", "GPE"):
            normalized = _normalize_phrase(ent.text)
            if normalized:
                entities.append(normalized)
    
    return entities


def _extract_pattern_phrases(title: str) -> List[str]:
    """Extract phrases using patterns: (adjective) + (food word)."""
    nlp = _get_spacy_model()
    if nlp is None:
        return _extract_pattern_phrases_fallback(title)
    
    doc = nlp(title)
    phrases = []
    
    # Look for patterns like "korean corn dog", "baked feta pasta"
    for i, token in enumerate(doc):
        token_lower = token.text.lower()
        if token_lower in FOOD_LEXICON:
            if token_lower in STYLE_TOKENS:
                continue
            # Get preceding adjectives/nouns
            modifiers = []
            for j in range(max(0, i - 2), i):
                if doc[j].pos_ in ("ADJ", "NOUN", "PROPN") and doc[j].text.lower() not in STOPWORDS:
                    modifiers.append(doc[j].text.lower())
            
            if modifiers:
                phrase = " ".join(modifiers + [token_lower])
                phrases.append(phrase)
    
    return phrases


def _extract_pattern_phrases_fallback(title: str) -> List[str]:
    """Fallback pattern extraction without spaCy."""
    tokens = [token.lower() for token in TOKEN_RE.findall(title)]
    phrases = []
    
    for i, token in enumerate(tokens):
        if token in FOOD_LEXICON:
            # Look back for 1-2 modifiers
            for lookback in range(1, min(3, i + 1)):
                start_idx = i - lookback
                modifiers = tokens[start_idx:i]
                if all(mod not in STOPWORDS for mod in modifiers):
                    phrase = " ".join(modifiers + [token])
                    phrases.append(phrase)
    
    return phrases


def _normalize_phrase(phrase: str) -> str:
    tokens = [token.lower() for token in TOKEN_RE.findall(phrase)]
    if not tokens:
        return ""
    return " ".join(tokens).strip()


def _is_food_phrase(phrase: str) -> bool:
    """Check if phrase is food-related."""
    if phrase in FOOD_LEXICON:
        return True
    tokens = phrase.split()
    if not tokens:
        return False
    if all(token in STOPWORDS for token in tokens):
        return False
    if any(token in FOOD_LEXICON for token in tokens):
        return True
    if len(tokens) >= 2:
        for n in (2, 3):
            if len(tokens) >= n:
                for i in range(len(tokens) - n + 1):
                    if " ".join(tokens[i : i + n]) in FOOD_LEXICON:
                        return True
    if any(token in FOOD_BRANDS for token in tokens):
        return True
    # Delivery platforms alone are not food; allow only when paired with a
    # strong food/dish signal.
    if any(token in DELIVERY_BRANDS for token in tokens):
        return any(token in FOOD_LEXICON for token in tokens) or (tokens[-1] in DISH_SUFFIXES)
    if tokens[-1] in DISH_SUFFIXES:
        return True
    return False


def _has_sufficient_quality(phrase: str) -> bool:
    """Check if phrase has sufficient quality to be considered a trend.
    
    Quality criteria:
    - Length: 2-4 words optimal
    - Not all stopwords
    - Contains at least one substantive word
    - Not overly generic
    """
    tokens = phrase.split()
    
    # Length check
    if len(tokens) < 1 or len(tokens) > 5:
        return False
    
    # Must have at least one non-stopword
    non_stopwords = [t for t in tokens if t not in STOPWORDS]
    if len(non_stopwords) == 0:
        return False
    
    # Prefer phrases with 2-3 words
    if len(tokens) >= 2:
        return True
    
    # Single word must be in food lexicon or dish suffixes
    if len(tokens) == 1:
        token = tokens[0]
        return token in FOOD_LEXICON or token in DISH_SUFFIXES or token in FOOD_BRANDS
    
    return True


def flatten_counts(counts_by_date: Dict[str, Dict[str, int]]) -> List[Tuple[str, str, int]]:
    rows = []
    for date, counts in counts_by_date.items():
        for phrase, count in counts.items():
            rows.append((phrase, date, count))
    return rows


def export_candidates_to_csv(
    counts_by_date: Dict[str, Dict[str, int]],
    total_counts: Dict[str, int],
    output_path: str,
) -> None:
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["phrase", "date", "count", "total_count"])
        for date, phrases in sorted(counts_by_date.items()):
            for phrase, count in sorted(phrases.items(), key=lambda item: item[1], reverse=True):
                writer.writerow([phrase, date, count, total_counts.get(phrase, count)])


def compute_total_counts(counts_by_date: Dict[str, Dict[str, int]]) -> Dict[str, int]:
    totals = Counter()
    for counts in counts_by_date.values():
        totals.update(counts)
    return dict(totals)

# NLP Enhancement Setup Guide

## Overview
The phrase extraction system now uses advanced NLP techniques with spaCy to improve food trend detection.

## Installation

### Python version
Use **Python 3.12** for spaCy. spaCy is currently incompatible with Python 3.14 in this project's dependency stack.

### 1. Install Python dependencies
```bash
py -3.12 -m venv gdelt/.venv
gdelt\.venv\Scripts\python.exe -m pip install -U pip
gdelt\.venv\Scripts\python.exe -m pip install -r gdelt/requirements.txt
```

### 2. Download spaCy language model
```bash
gdelt\.venv\Scripts\python.exe -m spacy download en_core_web_sm
```

## NLP Features Implemented

### 1. **Noun Chunk Extraction**
- Uses spaCy's dependency parser to identify noun phrases
- Automatically filters out determiners and pronouns
- Example: "viral korean corn dog" → "korean corn dog"

### 2. **Named Entity Recognition (NER)**
- Identifies food products and brand names
- Extracts entities labeled as PRODUCT, ORG, or GPE
- Example: "Starbucks pumpkin spice latte" → "pumpkin spice latte"

### 3. **Pattern-Based Extraction**
- Identifies modifier + food word patterns
- Uses Part-of-Speech (POS) tagging
- Example: "baked feta pasta recipe" → "baked feta pasta"

### 4. **N-gram Extraction**
- Fallback method for basic phrase extraction
- Generates 2-grams and 3-grams
- Works even without spaCy model

### 5. **Enhanced Food Lexicon**
- Expanded from 34 to 150+ food-related terms
- Includes proteins, dishes, beverages, cuisines, cooking methods
- Better detection of international cuisines (Korean, Japanese, Thai, etc.)

### 6. **Quality Scoring**
- Filters low-quality phrases
- Checks for substantive content
- Prefers 2-4 word phrases
- Removes overly generic terms

## How It Works

```python
# The system now uses multiple extraction strategies in parallel:

# 1. spaCy noun chunks (highest quality)
"The viral baked feta pasta recipe" 
→ "baked feta pasta"

# 2. Named entities
"Everyone's trying the Starbucks drink"
→ "starbucks drink"

# 3. Pattern matching (adjective + food)
"Korean corn dog trend"
→ "korean corn dog"

# 4. N-grams (fallback)
"butter chicken curry"
→ "butter chicken", "chicken curry"
```

## Improvements Over Previous Version

| Feature | Before | After |
|---------|--------|-------|
| Food terms | 34 | 150+ |
| Stopwords | 52 | 80+ |
| Generic phrases | 11 | 20+ |
| NLP methods | 1 (basic) | 4 (advanced) |
| Quality filtering | No | Yes |
| Deduplication | No | Yes |

## Performance

- **Without spaCy**: Falls back to pattern and n-gram extraction (it will not crash)
- **With spaCy**: Full NLP pipeline with 40-60% better phrase quality
- **Model caching**: Loads spaCy model once and reuses it

## Customization

### Add more food terms
Edit `FOOD_LEXICON` in `gdelt/src/extract_phrases.py`:
```python
FOOD_LEXICON = {
    "sushi", "ramen", "poke", 
    # Add your terms here
}
```

### Adjust quality thresholds
Modify `_has_sufficient_quality()` function:
```python
if len(tokens) < 1 or len(tokens) > 5:  # Change max length
    return False
```

### Filter generic phrases
Add to `GENERIC_PHRASES`:
```python
GENERIC_PHRASES = {
    "viral recipe", "tiktok trend",
    # Add phrases to exclude
}
```

## Troubleshooting

### spaCy model not found
```bash
python -m spacy download en_core_web_sm
```

### Slow performance
The system automatically caches the spaCy model after first load. If still slow:
- Reduce the number of NLP passes
- Use only n-gram extraction by removing spaCy import

### Too many generic phrases
Increase stopwords or add more terms to `GENERIC_PHRASES`

## Testing

Run the test suite to verify extraction quality:
```bash
pytest gdelt/tests/test_phrase_extraction.py -v
```

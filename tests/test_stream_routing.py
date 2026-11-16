"""
tests/test_stream_routing.py
----------------------------
Verifies AMQP topic pattern matching rules for Part D1 on paper and in CI.
"""

import re
import pytest


def topic_match(pattern: str, routing_key: str) -> bool:
    """
    AMQP 0-9-1 Topic Matching logic:
      - '*' matches exactly one dot-separated word.
      - '#' matches zero or more dot-separated words.
    """
    pattern_parts = pattern.split('.')
    regex_parts = []
    for part in pattern_parts:
        if part == '*':
            regex_parts.append(r'[^.]+')
        elif part == '#':
            regex_parts.append(r'(?:[^.]+(?:\.[^.]+)*)?')
        else:
            regex_parts.append(re.escape(part))

    # Construct regex
    regex_str = r'^\.' + r'\.'.join(regex_parts) + r'\.$'
    # Handle # matching empty: replace double dots
    key_padded = f".{routing_key}."
    
    # Standard AMQP word-by-word token comparison
    p_tokens = pattern.split('.')
    k_tokens = routing_key.split('.')

    def match_tokens(p_idx, k_idx):
        if p_idx == len(p_tokens) and k_idx == len(k_tokens):
            return True
        if p_idx < len(p_tokens) and p_tokens[p_idx] == '#':
            # Match 0 words or more
            for skip in range(len(k_tokens) - k_idx + 1):
                if match_tokens(p_idx + 1, k_idx + skip):
                    return True
            return False
        if p_idx < len(p_tokens) and k_idx < len(k_tokens):
            if p_tokens[p_idx] == '*' or p_tokens[p_idx] == k_tokens[k_idx]:
                return match_tokens(p_idx + 1, k_idx + 1)
        return False

    return match_tokens(0, 0)


# Drivers from Part D1
DRIVERS = {
    'Ravi': 'bangalore.koramangala.*',
    'Priya': 'bangalore.indiranagar.*',
    'Arjun': 'bangalore.*.ride',
    'Meena': 'bangalore.*.food',
    'Karthik': 'chennai.tnagar.food',
    'Control': 'bangalore.#',
}


def test_d1_key_1_koramangala_food():
    key = 'bangalore.koramangala.food'
    matched = [d for d, pat in DRIVERS.items() if topic_match(pat, key)]
    assert sorted(matched) == sorted(['Ravi', 'Meena', 'Control'])


def test_d1_key_2_indiranagar_ride():
    key = 'bangalore.indiranagar.ride'
    matched = [d for d, pat in DRIVERS.items() if topic_match(pat, key)]
    assert sorted(matched) == sorted(['Priya', 'Arjun', 'Control'])


def test_d1_key_3_whitefield_ride():
    key = 'bangalore.whitefield.ride'
    matched = [d for d, pat in DRIVERS.items() if topic_match(pat, key)]
    assert sorted(matched) == sorted(['Arjun', 'Control'])


def test_d1_key_4_chennai_tnagar_food():
    key = 'chennai.tnagar.food'
    matched = [d for d, pat in DRIVERS.items() if topic_match(pat, key)]
    assert matched == ['Karthik']


def test_d1_key_5_chennai_whitefield_ride():
    key = 'chennai.whitefield.ride'
    matched = [d for d, pat in DRIVERS.items() if topic_match(pat, key)]
    assert matched == []  # dropped


def test_d1_key_6_bangalore_koramangala_two_words():
    key = 'bangalore.koramangala'
    matched = [d for d, pat in DRIVERS.items() if topic_match(pat, key)]
    # Control matches because # matches 0 or more words
    # Ravi fails because * requires exactly one 3rd word
    assert matched == ['Control']

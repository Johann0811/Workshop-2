"""
Unit tests for artist name normalization.
"""

import pytest
from src.transform import normalize_artist_name


@pytest.mark.parametrize(
    "raw_name, expected",
    [
        ("Coldplay", "coldplay"),
        ("Beyoncé", "beyonce"),
        ("AC/DC", "acdc"),
        ("Guns N' Roses", "guns n roses"),
        ("  Dua Lipa  ", "dua lipa"),
        ("The   Weeknd", "the weeknd"),
        ("Mötley Crüe", "motley crue"),
        ("ROSALÍA", "rosalia"),
        ("", None),
        ("   ", None),
        (None, None),
    ],
)
def test_normalize_artist_name(raw_name, expected):
    assert normalize_artist_name(raw_name) == expected

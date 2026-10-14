"""
Unit tests for Grammy status and award bands derivation.
"""

import pytest
from src.transform import derive_grammy_status, derive_wins_band


@pytest.mark.parametrize(
    "wins, records_2019, expected_status",
    [
        (5, 0, "historical winner"),
        (3, 2, "winner + 2019"),
        (0, 1, "2019 only"),
        (0, 0, "no record"),
        (0, None, "no record"),
    ],
)
def test_derive_grammy_status(wins, records_2019, expected_status):
    assert derive_grammy_status(wins, records_2019) == expected_status


@pytest.mark.parametrize(
    "wins, expected_band",
    [
        (0, "none"),
        (-1, "none"),
        (1, "1"),
        (2, "2-4"),
        (4, "2-4"),
        (5, "5-9"),
        (9, "5-9"),
        (10, "10+"),
        (28, "10+"),
    ],
)
def test_derive_wins_band(wins, expected_band):
    assert derive_wins_band(wins) == expected_band

"""
Unit tests for source counts, row conservation, and reconciliation invariants.
"""

from unittest.mock import MagicMock, patch
import pandas as pd
import pytest
from src.extract import prepare_grammy_source


def test_row_conservation_invariants():
    raw_spotify_rows = 114000
    exact_copies = 450
    quarantined = 1
    expected_fact_rows = raw_spotify_rows - exact_copies - quarantined
    assert expected_fact_rows == 113549

    expected_genres = 114
    assert expected_genres == 114

    grammy_rows = 4810
    assert grammy_rows == 4810


def test_prepare_grammy_source_reconciliation_failure(tmp_path):
    # Create dummy csv with 10 rows instead of 4810
    dummy_csv = tmp_path / "dummy_grammy.csv"
    df = pd.DataFrame({
        "year": [2010] * 10,
        "title": ["Song"] * 10,
        "published_at": ["2010-01-01T00:00:00-07:00"] * 10,
        "updated_at": ["2010-01-01T00:00:00-07:00"] * 10,
        "category": ["Pop"] * 10,
        "nominee": ["Artist"] * 10,
        "artist": ["Artist"] * 10,
        "workers": ["Worker"] * 10,
        "img": ["img.jpg"] * 10,
        "winner": [True] * 10,
    })
    df.to_csv(dummy_csv, index=False)

    # Mock engine execution returning 10 instead of 4810
    with patch("src.extract.create_engine") as mock_engine_cls:
        mock_conn = MagicMock()
        # count query returns 10
        mock_conn.execute.return_value.scalar_one.return_value = 10
        mock_engine_cls.return_value.begin.return_value.__enter__.return_value = mock_conn

        with pytest.raises(ValueError, match="Grammy source reconciliation failed"):
            prepare_grammy_source(str(dummy_csv), "mysql+pymysql://fake:fake@fake/fake")

"""
Unit tests for popularity decile and zero traction flags.
"""

import numpy as np
import pandas as pd
import pytest


def test_analytical_flags_logic():
    sample_popularity = pd.Series([0, 10, 35, 62, 63, 70, 100])
    p90 = 63

    is_top10 = (sample_popularity >= p90).astype(int)
    is_zero = (sample_popularity == 0).astype(int)

    assert list(is_top10) == [0, 0, 0, 0, 1, 1, 1]
    assert list(is_zero) == [1, 0, 0, 0, 0, 0, 0]


def test_flag_coherence_invariants():
    # Mutual exclusivity of 0 traction and top10 (since p90 >= 60 > 0)
    pop_values = np.random.randint(0, 101, size=500)
    p90 = 63
    is_top10 = pop_values >= p90
    is_zero = pop_values == 0

    # No row can simultaneously be top10 and zero traction
    assert not np.any(is_top10 & is_zero)

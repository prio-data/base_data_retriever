from unittest import TestCase
import numpy as np
from base_data_retriever import checks

class TestChecks(TestCase):
    def test_is_monotonic(self):
        self.assertFalse(checks.is_monotonic(np.array([1,3,4])).value)
        self.assertTrue(checks.is_monotonic(np.array([1,2,3])).value)

        self.assertTrue(
            checks.is_monotonic(np.linspace(1,10,10).astype(int)).value
                )

        self.assertTrue(
            checks.is_monotonic(np.linspace(21,30,10).astype(int)).value
                )

        self.assertTrue(checks.is_monotonic(np.linspace(1,100,5)).is_left)

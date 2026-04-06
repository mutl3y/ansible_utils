# (c) 2024, Mark Heynes <mark.heynes@heynesit.co.uk>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
"""Unit tests for plugins/lookup/excel_merge.py.

These tests exercise the pure helper functions and the private methods of
LookupModule directly, without requiring a real Ansible runtime or an actual
Excel file on disk.
"""

from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import MagicMock, patch

import pandas


# ---------------------------------------------------------------------------
# Minimal Ansible stubs so the module can be imported without ansible-core
# ---------------------------------------------------------------------------

def _make_ansible_stubs():
    """Insert lightweight stub modules into sys.modules."""
    # ansible
    ansible_pkg = types.ModuleType("ansible")

    # ansible.errors
    errors_mod = types.ModuleType("ansible.errors")

    class AnsibleError(Exception):
        pass

    class AnsibleOptionsError(AnsibleError):
        pass

    class AnsibleLookupError(AnsibleError):
        pass

    errors_mod.AnsibleError = AnsibleError
    errors_mod.AnsibleOptionsError = AnsibleOptionsError
    errors_mod.AnsibleLookupError = AnsibleLookupError

    # ansible.module_utils.common.text.converters
    converters_mod = types.ModuleType("ansible.module_utils.common.text.converters")
    converters_mod.to_native = str

    # ansible.plugins.lookup
    lookup_mod = types.ModuleType("ansible.plugins.lookup")

    class LookupBase:
        def set_options(self, var_options=None, direct=None):
            pass

        def get_options(self):
            return {}

        def find_file_in_search_path(self, variables, subdir, name):
            return name

    lookup_mod.LookupBase = LookupBase

    # ansible.utils.display
    display_mod = types.ModuleType("ansible.utils.display")

    class Display:
        def v(self, msg):
            pass

        def vvv(self, msg):
            pass

        def vvvv(self, msg):
            pass

        def warning(self, msg):
            pass

    display_mod.Display = Display

    # Register all the sub-packages that are required
    for name, mod in [
        ("ansible", ansible_pkg),
        ("ansible.errors", errors_mod),
        ("ansible.module_utils", types.ModuleType("ansible.module_utils")),
        ("ansible.module_utils.common", types.ModuleType("ansible.module_utils.common")),
        ("ansible.module_utils.common.text", types.ModuleType("ansible.module_utils.common.text")),
        ("ansible.module_utils.common.text.converters", converters_mod),
        ("ansible.plugins", types.ModuleType("ansible.plugins")),
        ("ansible.plugins.lookup", lookup_mod),
        ("ansible.utils", types.ModuleType("ansible.utils")),
        ("ansible.utils.display", display_mod),
    ]:
        sys.modules.setdefault(name, mod)

    return errors_mod


_ansible_errors = _make_ansible_stubs()

# Now import the module under test
import importlib
excel_merge = importlib.import_module("plugins.lookup.excel_merge")

LookupModule = excel_merge.LookupModule
_trim_dataframe = excel_merge._trim_dataframe
AnsibleLookupError = _ansible_errors.AnsibleLookupError
AnsibleOptionsError = _ansible_errors.AnsibleOptionsError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _str_df(**columns: list[str]) -> pandas.DataFrame:
    """Build a pandas DataFrame with StringDtype columns."""
    return pandas.DataFrame(
        {k: pandas.array(v, dtype="string") for k, v in columns.items()}
    )


def _make_lookup() -> LookupModule:
    """Return an uninitialised LookupModule instance."""
    return LookupModule()  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# _trim_dataframe
# ---------------------------------------------------------------------------

class TestTrimDataframe(unittest.TestCase):
    def test_strips_column_names(self):
        # Keys intentionally contain leading/trailing spaces to exercise trimming.
        df = _str_df(**{" env ": ["deva"], " name ": ["host1"]})
        result = _trim_dataframe(df)
        self.assertListEqual(list(result.columns), ["env", "name"])

    def test_strips_string_cell_values(self):
        df = _str_df(env=["  deva  ", " devb"])
        result = _trim_dataframe(df)
        self.assertEqual(result["env"][0], "deva")
        self.assertEqual(result["env"][1], "devb")

    def test_non_string_columns_unchanged(self):
        df = pandas.DataFrame({"count": [1, 2, 3]})
        result = _trim_dataframe(df)
        self.assertListEqual(list(result["count"]), [1, 2, 3])


# ---------------------------------------------------------------------------
# LookupModule._apply_filter
# ---------------------------------------------------------------------------

class TestApplyFilter(unittest.TestCase):
    def setUp(self):
        self.lm = _make_lookup()
        self.df = _str_df(
            env=["deva", "devb", "deva", "devc"],
            name=["host1", "host2", "host3", "host4"],
        )

    def test_exact_match(self):
        result = self.lm._apply_filter(self.df, "env", "deva", partial_match=False)
        self.assertEqual(len(result), 2)
        self.assertTrue((result["env"] == "deva").all())

    def test_exact_match_no_results(self):
        result = self.lm._apply_filter(self.df, "env", "devz", partial_match=False)
        self.assertEqual(len(result), 0)

    def test_partial_match(self):
        result = self.lm._apply_filter(self.df, "env", "dev", partial_match=True)
        self.assertEqual(len(result), 4)

    def test_partial_match_substring(self):
        result = self.lm._apply_filter(self.df, "env", "deva", partial_match=True)
        self.assertEqual(len(result), 2)

    def test_missing_filter_col_raises(self):
        with self.assertRaises(AnsibleLookupError):
            self.lm._apply_filter(self.df, "nonexistent", "deva")

    def test_nan_cells_do_not_crash(self):
        df = _str_df(env=["deva", pandas.NA, "devb"])
        result = self.lm._apply_filter(df, "env", "deva", partial_match=False)
        self.assertEqual(len(result), 1)

    def test_partial_match_nan_cells_do_not_crash(self):
        df = _str_df(env=["deva", pandas.NA, "devb"])
        result = self.lm._apply_filter(df, "env", "dev", partial_match=True)
        self.assertEqual(len(result), 2)


# ---------------------------------------------------------------------------
# LookupModule._merge_dataframes
# ---------------------------------------------------------------------------

class TestMergeDataframes(unittest.TestCase):
    def setUp(self):
        self.lm = _make_lookup()
        self.infra = _str_df(env=["deva", "devb"], name=["h1", "h2"], ip=["1.1.1.1", "1.2.1.1"])
        self.app = _str_df(env=["deva", "devb"], name=["h1", "h2"], Xmx=["4096", "2048"])

    def test_single_dataframe_returned_as_is(self):
        result = self.lm._merge_dataframes([self.infra], "left", None)
        self.assertEqual(len(result), 2)
        self.assertIn("ip", result.columns)

    def test_merge_with_join_on(self):
        result = self.lm._merge_dataframes([self.infra, self.app], "inner", ["env", "name"])
        self.assertEqual(len(result), 2)
        self.assertIn("ip", result.columns)
        self.assertIn("Xmx", result.columns)

    def test_merge_without_join_on_uses_common_columns(self):
        # Both frames share env+name; pandas should merge on those automatically.
        result = self.lm._merge_dataframes([self.infra, self.app], "inner", None)
        self.assertIn("ip", result.columns)
        self.assertIn("Xmx", result.columns)

    def test_cross_join(self):
        a = _str_df(x=["1", "2"])
        b = _str_df(y=["a", "b"])
        result = self.lm._merge_dataframes([a, b], "cross", None)
        self.assertEqual(len(result), 4)

    def test_empty_list_raises(self):
        with self.assertRaises(AnsibleOptionsError):
            self.lm._merge_dataframes([], "left", None)


# ---------------------------------------------------------------------------
# LookupModule._select_columns
# ---------------------------------------------------------------------------

class TestSelectColumns(unittest.TestCase):
    def setUp(self):
        self.lm = _make_lookup()
        self.df = _str_df(env=["deva"], name=["h1"], ip=["1.1.1.1"])

    def test_keeps_requested_columns(self):
        result = self.lm._select_columns(self.df, ["env", "ip"], None)
        self.assertListEqual(list(result.columns), ["env", "ip"])

    def test_appends_filter_col_if_missing(self):
        result = self.lm._select_columns(self.df, ["ip"], "env")
        self.assertIn("env", result.columns)
        self.assertIn("ip", result.columns)

    def test_does_not_duplicate_filter_col(self):
        result = self.lm._select_columns(self.df, ["env", "ip"], "env")
        self.assertEqual(list(result.columns).count("env"), 1)

    def test_missing_column_raises(self):
        with self.assertRaises(AnsibleLookupError):
            self.lm._select_columns(self.df, ["nonexistent"], None)


# ---------------------------------------------------------------------------
# LookupModule._normalize_nan
# ---------------------------------------------------------------------------

class TestNormalizeNan(unittest.TestCase):
    def setUp(self):
        self.lm = _make_lookup()

    def test_sentinel_nan_leaves_dataframe_unchanged(self):
        df = _str_df(env=["deva", pandas.NA])
        result = self.lm._normalize_nan(df, "nan")
        self.assertTrue(pandas.isna(result["env"][1]))

    def test_replacement_fills_nan(self):
        df = _str_df(env=["deva", pandas.NA])
        result = self.lm._normalize_nan(df, "")
        self.assertEqual(result["env"][1], "")


if __name__ == "__main__":
    unittest.main()

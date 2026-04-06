"""Unit tests for plugins/lookup/excel_sheet.py."""

from __future__ import annotations

import importlib
import sys
import types
import unittest

import pandas


def _make_ansible_stubs():
    ansible_pkg = types.ModuleType("ansible")
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

    converters_mod = types.ModuleType("ansible.module_utils.common.text.converters")
    converters_mod.to_native = str

    lookup_mod = types.ModuleType("ansible.plugins.lookup")

    class LookupBase:
        def set_options(self, var_options=None, direct=None):
            pass

        def get_options(self):
            return {}

        def find_file_in_search_path(self, variables, subdir, name):
            return name

    lookup_mod.LookupBase = LookupBase

    display_mod = types.ModuleType("ansible.utils.display")

    class Display:
        def v(self, msg):
            pass

    display_mod.Display = Display

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


_make_ansible_stubs()
excel_sheet = importlib.import_module("plugins.lookup.excel_sheet")

LookupModule = excel_sheet.LookupModule
trim_dataframe = excel_sheet._trim_dataframe
AnsibleLookupError = excel_sheet.AnsibleLookupError
AnsibleOptionsError = excel_sheet.AnsibleOptionsError


def _str_df(**columns) -> pandas.DataFrame:
    return pandas.DataFrame({k: pandas.array(v, dtype="string") for k, v in columns.items()})


class TestTrimDataframe(unittest.TestCase):
    def test_trim_columns_and_values(self):
        dataframe = _str_df(**{" env ": [" deva "], " name ": [" host1 "]})
        result = trim_dataframe(dataframe)
        self.assertEqual(list(result.columns), ["env", "name"])
        self.assertEqual(result["env"][0], "deva")


class TestLookupHelpers(unittest.TestCase):
    def setUp(self):
        self.lookup = LookupModule()  # type: ignore[call-arg]
        self.df = _str_df(env=["deva", "devb"], ip=["1.1.1.1", "1.2.1.1"], name=["h1", "h2"])

    def test_validate_params_requires_sheet(self):
        with self.assertRaises(AnsibleOptionsError):
            self.lookup._validate_params({"sheet": None})

    def test_apply_filter_exact(self):
        result = self.lookup._apply_filter(self.df, "env", "deva")
        self.assertEqual(len(result), 1)
        self.assertEqual(result["env"].iloc[0], "deva")

    def test_apply_filter_missing_col(self):
        with self.assertRaises(AnsibleLookupError):
            self.lookup._apply_filter(self.df, "missing", "deva")

    def test_select_columns_keeps_filter_col(self):
        result = self.lookup._select_columns(self.df, ["ip"], "env")
        self.assertEqual(list(result.columns), ["ip", "env"])

    def test_select_columns_missing_raises(self):
        with self.assertRaises(AnsibleLookupError):
            self.lookup._select_columns(self.df, ["missing"], None)

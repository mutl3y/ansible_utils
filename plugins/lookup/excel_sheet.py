# (c) 2024, Mark Heynes <mark.heynes@heynesit.co.uk>
# (c) 2024 HeynesIT
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
""" excel_sheet.py """

from __future__ import annotations

from typing import Any

from ansible.errors import AnsibleError, AnsibleLookupError, AnsibleOptionsError
from ansible.module_utils.common.text.converters import to_native
from ansible.plugins.lookup import LookupBase
from ansible.utils.display import Display
import pandas

DOCUMENTATION = r"""
    name: excel_sheet
    author: Mark Heynes <mark.heynes@heynesit.co.uk>
    short_description: read data from named sheet in XLSX file
    description:
      - The excel_sheet lookup reads the contents of a named sheet from an XLSX
        (Excel Open XML Spreadsheet) format file.
    options:
      sheet:
        description: Name of the sheet to read.
        type: string
        required: true
      cols:
        description:
          - Restrict returned columns to this list.
          - O(filter_col) is always included when specified.
        default: []
        type: list
      filter_col:
        description: Column name to filter on (used with O(filter)).
        default: null
        type: string
      filter:
        description: Exact value to keep in O(filter_col).
        default: null
        type: string
      default:
        description:
          - Additional NA marker value passed to C(pandas.read_excel) via C(na_values).
          - Set this to control which value should be treated as empty in addition to pandas defaults.
        default: ''
      file:
        description: Name of the XLSX file to open.
        default: null
        required: true
        type: path
    notes:
      - If O(cols) is not specified all columns are returned.
"""

EXAMPLES = """
- name: Match 'deva' on 'env' column and return 'ip'
  ansible.builtin.debug:
    msg: >-
      {{ lookup('mutl3y.utils.excel_sheet', file='sample.xlsx',
         sheet='infra', filter='deva', filter_col='env', cols=['ip']) }}
"""

RETURN = """
  _raw:
    description:
      - List of dicts, one per row, where keys are column names and values are cell values.
    type: list
    elements: dict
"""

display = Display()


class LookupModule(LookupBase):
    """ lookup module """

    def run(self, terms: list, variables: dict | None = None, **kwargs: Any) -> list[dict]:
        """run method"""
        self.set_options(var_options=variables, direct=kwargs)
        paramvals: dict = self.get_options()
        display.v("excel_sheet parameters: %s" % to_native(paramvals))

        self._validate_params(paramvals)
        lookupfile = self.find_file_in_search_path(variables, 'files', paramvals['file'])
        if not lookupfile:
            raise AnsibleError(
                "excel_sheet: file %r not found in the configured search path"
                % to_native(paramvals['file'])
            )

        try:
            dataframe = self._read_sheet(lookupfile, paramvals['sheet'], paramvals['default'])
            dataframe = _trim_dataframe(dataframe)
            dataframe = self._apply_filter(
                dataframe, paramvals.get('filter_col'), paramvals.get('filter')
            )
            dataframe = self._select_columns(
                dataframe, paramvals.get('cols') or [], paramvals.get('filter_col')
            )

            if len(dataframe) == 0:
                raise AnsibleLookupError(
                    'excel_sheet: no data rows left to return, review filters and source data'
                )

            return dataframe.to_dict(orient='records')

        except (AnsibleError, AnsibleLookupError, AnsibleOptionsError):
            raise
        except FileNotFoundError as exc:
            raise AnsibleError(
                "excel_sheet: file %r not found: %s"
                % (to_native(paramvals['file']), to_native(exc))
            ) from exc
        except Exception as exc:
            raise AnsibleError("excel_sheet: unexpected error: %s" % to_native(exc)) from exc

    def _validate_params(self, paramvals: dict) -> None:
        if not paramvals.get('sheet'):
            raise AnsibleOptionsError("excel_sheet: 'sheet' must be provided")

    def _read_sheet(
        self, lookupfile: str, sheet_name: str, default_value: Any
    ) -> pandas.DataFrame:
        try:
            return pandas.read_excel(
                lookupfile,
                dtype='string',
                na_values=default_value,
                keep_default_na=False,
                sheet_name=sheet_name,
            )
        except ValueError as exc:
            raise AnsibleLookupError(
                "excel_sheet: sheet %r not found in %r: %s"
                % (sheet_name, to_native(lookupfile), to_native(exc))
            ) from exc

    def _apply_filter(
        self, dataframe: pandas.DataFrame, filter_col: str | None, filter_value: str | None
    ) -> pandas.DataFrame:
        if not (filter_col and filter_value):
            return dataframe
        if filter_col not in dataframe.columns:
            raise AnsibleLookupError(
                "excel_sheet: filter_col %r not found; available columns: %s"
                % (filter_col, to_native(list(dataframe.columns)))
            )
        mask = dataframe[filter_col] == filter_value
        return dataframe.loc[mask]

    def _select_columns(
        self, dataframe: pandas.DataFrame, columns: list[str], filter_col: str | None
    ) -> pandas.DataFrame:
        if not columns:
            return dataframe
        keep = list(columns)
        if filter_col and filter_col not in keep:
            keep.append(filter_col)
        missing = [column for column in keep if column not in dataframe.columns]
        if missing:
            raise AnsibleLookupError(
                "excel_sheet: requested column(s) %s not found; available: %s"
                % (missing, to_native(list(dataframe.columns)))
            )
        return dataframe.loc[:, keep]


def _trim_dataframe(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    dataframe = dataframe.rename(columns={column: column.strip() for column in dataframe.columns})
    for column in dataframe.columns:
        if dataframe[column].dtype == 'string':
            dataframe[column] = dataframe[column].str.strip()
    return dataframe

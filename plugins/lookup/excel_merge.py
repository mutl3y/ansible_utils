# (c) 2024, Mark Heynes <mark.heynes@heynesit.co.uk>
# (c) 2024 HeynesIT
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
""" excel_merge.py """

from __future__ import annotations

from typing import Any

from ansible.errors import AnsibleError, AnsibleOptionsError, AnsibleLookupError
from ansible.module_utils.common.text.converters import to_native
from ansible.plugins.lookup import LookupBase
from ansible.utils.display import Display
import pandas

DOCUMENTATION = r"""
    name: excel_merge
    author: Mark Heynes <mark.heynes@heynesit.co.uk>
    short_description: read data from multiple sheets in XLSX file
    description:
      - The excel_merge lookup reads the contents of multiple sheets from an XLSX
        (Excel Open XML Spreadsheet) format file and merges them into a single result.
      - All values are returned as string data.
    options:
      sheets:
        description:
          - List of sheet names to read and merge, e.g. C(['Sheet1', 'Sheet2']).
          - At least one sheet name must be provided.
        type: list
        required: true
      cols:
        description:
          - Restrict the returned columns to this list.
          - O(filter_col) is always included in the output when specified.
          - If omitted, all columns are returned.
        type: list
      filter_col:
        description: Column name to apply the filter against.
        type: string
      filter:
        description: Value to filter rows by. Used together with O(filter_col).
        type: string
      filter_partial_match:
        description:
          - When V(true), keep rows where O(filter) appears as a substring of the
            O(filter_col) cell value (case-sensitive).
          - When V(false) (default), keep only rows where the cell exactly equals O(filter).
        aliases: [ part ]
        type: bool
        default: false
      join_type:
        description:
          - Type of pandas merge join to use when combining multiple sheets.
          - V(cross) is mutually exclusive with O(join_on).
        default: left
        choices: [ left, right, outer, inner, cross ]
      join_on:
        description:
          - Column name(s) to use as join keys when merging sheets.
          - When omitted, pandas merges on all column names that are common to
            both sheets (in the order they appear).
          - Mutually exclusive with V(cross) join type.
      trim:
        description: Trim leading and trailing whitespace from column names and string cell values.
        default: true
        type: bool
      nan:
        description:
          - Replacement value for empty (NaN) cells in the final dataframe.
          - When set to the string V(nan) (the default), NaN cells are left as-is.
          - Set to V('') to return empty strings, or any other string/numeric value.
        default: nan
      file:
        description: Path to the XLSX file to open.
        required: true
        type: path
    notes:
      - If O(cols) is not specified, all columns are returned.
"""

display = Display()

_VALID_JOIN_TYPES = frozenset({"left", "right", "outer", "inner", "cross"})


class LookupModule(LookupBase):
    """Ansible lookup module: excel_merge."""

    def run(self, terms: list, variables: dict | None = None, **kwargs: Any) -> list[dict]:
        """Execute the lookup and return a list of row dicts."""
        self.set_options(var_options=variables, direct=kwargs)
        param_map: dict = self.get_options()
        display.v("excel_merge parameters: %s" % to_native(param_map))

        # --- parameter validation -------------------------------------------
        sheets: list = param_map.get("sheets") or []
        if not sheets:
            raise AnsibleOptionsError("excel_merge: 'sheets' must be a non-empty list")

        join_type: str = param_map["join_type"]
        if join_type not in _VALID_JOIN_TYPES:
            raise AnsibleOptionsError(
                "excel_merge: invalid join_type %r; must be one of %s"
                % (join_type, sorted(_VALID_JOIN_TYPES))
            )

        join_on = param_map.get("join_on")
        if join_type == "cross" and join_on is not None:
            raise AnsibleOptionsError(
                "excel_merge: 'join_type: cross' and 'join_on' are mutually exclusive"
            )

        lookupfile = self.find_file_in_search_path(variables, "files", param_map["file"])
        if not lookupfile:
            raise AnsibleError(
                "excel_merge: file %r not found in the configured search path"
                % to_native(param_map["file"])
            )

        try:
            dfs = self._read_sheets(lookupfile, sheets, param_map["trim"])
            dataframe = self._merge_dataframes(dfs, join_type, join_on)

            filter_val = param_map.get("filter")
            filter_col = param_map.get("filter_col")
            if filter_val and filter_col:
                dataframe = self._apply_filter(
                    dataframe,
                    filter_col,
                    filter_val,
                    partial_match=param_map.get("filter_partial_match", False),
                )

            cols = param_map.get("cols")
            if cols:
                dataframe = self._select_columns(dataframe, cols, filter_col)

            if len(dataframe) == 0:
                display.warning("excel_merge: no data rows left to return")

            dataframe = self._normalize_nan(dataframe, param_map["nan"])
            return dataframe.to_dict(orient="records")

        except (AnsibleError, AnsibleOptionsError, AnsibleLookupError):
            raise
        except FileNotFoundError as exc:
            raise AnsibleError(
                "excel_merge: file %r not found: %s"
                % (to_native(param_map["file"]), to_native(exc))
            ) from exc
        except Exception as exc:
            raise AnsibleError(
                "excel_merge: unexpected error: %s" % to_native(exc)
            ) from exc

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _read_sheets(
        self, path: str, sheets: list[str], trim: bool
    ) -> list[pandas.DataFrame]:
        """Read each sheet from *path* and return a list of DataFrames."""
        dfs: list[pandas.DataFrame] = []
        for sheet in sheets:
            try:
                df = pandas.read_excel(path, dtype="string", sheet_name=sheet)
            except ValueError as exc:
                raise AnsibleLookupError(
                    "excel_merge: sheet %r not found in %r: %s"
                    % (sheet, to_native(path), to_native(exc))
                ) from exc
            except Exception as exc:
                raise AnsibleError(
                    "excel_merge: error reading sheet %r from %r: %s"
                    % (sheet, to_native(path), to_native(exc))
                ) from exc
            if trim:
                df = _trim_dataframe(df)
            display.vvv(
                "excel_merge: read sheet %r — %d rows, columns: %s"
                % (sheet, len(df), list(df.columns))
            )
            dfs.append(df)
        return dfs

    def _merge_dataframes(
        self,
        dfs: list[pandas.DataFrame],
        join_type: str,
        join_on: list[str] | str | None,
    ) -> pandas.DataFrame:
        """Merge a list of DataFrames sequentially with the given join parameters."""
        if not dfs:
            raise AnsibleOptionsError("excel_merge: no dataframes to merge")

        result = dfs[0]
        for df in dfs[1:]:
            try:
                result = result.merge(df, how=join_type, on=join_on)
            except Exception as exc:
                raise AnsibleError(
                    "excel_merge: merge failed (join_type=%r, join_on=%r): %s"
                    % (join_type, join_on, to_native(exc))
                ) from exc
            display.vvvv(
                "excel_merge: after merge — %d rows, columns: %s"
                % (len(result), list(result.columns))
            )
        return result

    def _apply_filter(
        self,
        dataframe: pandas.DataFrame,
        filter_col: str,
        filter_val: str,
        partial_match: bool = False,
    ) -> pandas.DataFrame:
        """Return a filtered copy of *dataframe* based on the filter parameters."""
        if filter_col not in dataframe.columns:
            raise AnsibleLookupError(
                "excel_merge: filter_col %r not found; available columns: %s"
                % (filter_col, to_native(list(dataframe.columns)))
            )

        col = dataframe[filter_col]
        if partial_match:
            mask = col.str.contains(filter_val, na=False)
        else:
            # For StringDtype, NA == filter_val returns NA which is treated
            # as False when used as a boolean index, so no special handling needed.
            mask = col == filter_val

        filtered = dataframe.loc[mask]
        display.vvv(
            "excel_merge: filter %r=%r (partial=%s) kept %d/%d rows"
            % (filter_col, filter_val, partial_match, len(filtered), len(dataframe))
        )
        return filtered

    def _select_columns(
        self,
        dataframe: pandas.DataFrame,
        cols: list[str],
        filter_col: str | None,
    ) -> pandas.DataFrame:
        """Return a copy of *dataframe* with only the requested columns."""
        keep = list(cols)
        if filter_col and filter_col not in keep:
            keep.append(filter_col)

        missing = [c for c in keep if c not in dataframe.columns]
        if missing:
            raise AnsibleLookupError(
                "excel_merge: requested column(s) %s not found; available: %s"
                % (missing, to_native(list(dataframe.columns)))
            )

        display.vvv("excel_merge: selecting columns %s" % keep)
        return dataframe.loc[:, keep]

    def _normalize_nan(
        self, dataframe: pandas.DataFrame, nan_value: Any
    ) -> pandas.DataFrame:
        """Fill NaN cells with *nan_value* if it is not the sentinel string 'nan'."""
        if nan_value != "nan":
            return dataframe.fillna(value=nan_value)
        return dataframe


def _trim_dataframe(df: pandas.DataFrame) -> pandas.DataFrame:
    """Trim whitespace from column names and all string-typed cells."""
    df = df.rename(columns={c: c.strip() for c in df.columns})
    for col in df.columns:
        if df[col].dtype == "string":
            df[col] = df[col].str.strip()
    return df


EXAMPLES = """
- name: Filter by env and return selected columns
  ansible.builtin.debug:
    msg: >-
      {{ lookup('mutl3y.utils.excel_merge', file='sample.xlsx',
         sheets=['infra', 'app_config'],
         filter='deva', filter_col='env', cols=['name', 'ip']) }}

# Contents of sample.xlsx shown in CSV format for simplicity.
# sheet_name="infra"
#
# env,name,ip,ram,first_disk
# deva,deva-dcb-123t,1.1.1.1,128,40
# deva,deva-ncs-124t,1.1.2.2,64,35
# devb,abc-dcb-223t,1.2.1.1,46,35
#
# sheet_name="app_config"
#
# env,name,Xmx,Xms,Xss
# deva,deva-dcb-123t,4096,1024,128
# deva,deva-ncs-123t,3218,512,64

- name: Merge on explicit keys, inner join
  ansible.builtin.debug:
    msg: >-
      {{ lookup('mutl3y.utils.excel_merge', file='sample.xlsx',
         sheets=['infra', 'app_config'],
         join_on=['env', 'name'], join_type='inner') }}
"""

RETURN = """
  _raw:
    description:
      - List of dicts, one per row, where keys are column names and values are cell values.
    type: list
    elements: dict
"""

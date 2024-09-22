# (c) 2024, Mark Heynes <mark.heynes@heynesit.co.uk>
# (c) 2024 HeynesIT
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
""" excel_merge.py """

from __future__ import annotations
from ansible.errors import AnsibleError
from ansible.plugins.lookup import LookupBase
from ansible.utils.display import Display
import pandas

DOCUMENTATION = r"""
    name: excel_merge
    author: Mark Heynes <mark.heynes@heynesit.co.uk>
    short_description: read data from multiple sheets in XLSX file
    description:
      - The excel_merge lookup reads the contents of multiple sheets from an XLSX (Excel Open XML Spreadsheet) format 
        file.
      - All values are returned as string data
    options:
      sheets:
        description: List of sheets to return data from ['Sheet1', 'Sheet 2']
        type: list
      cols:
        description: Restrict returned columns to just these + filter_col.
        type: list
      filter_col:
        description: Column to filter on.
        type: string
      filter:
        description: Text to filter data on.
        type: string
      join_type:
        description: Type of join to use in merge
        default: 'left'
        choices:
          - 'left'
          - 'right'
          - 'outer'
          - 'inner'
          - 'cross'
      join_on:
        description: 
          - Column names to use in join. 
          - Defaults to common keys between sheets in the order they are found
          - Mutually exclusive with the option cross of O(join_type).
      trim:
        description: Trim leading and trailing spaces from keys and values
        default: True
        type: bool
      default:
        description: What to return if the cell is empty.
        default: NA
      file:
        description: Name of the XLSX file to open.
        required: true
    notes:
      - if cols is not specified all columns are returned
"""

EXAMPLES = """
- name: msg="Match 'deva' on the 'env' column, but return the 'ip' column"
  ansible.builtin.debug: 
    msg="The ips in deva are {{ lookup('ansible.legacy.excel_merge', file='sample.xlsx', sheets=['infra', 'app_config'], 
    filter='deva', filter_col='env', col='hostname') }}"

# Contents of sample2.xlsx shown in csv format for simplicity
sheet_name="infra"

env, name, ip, ram, first_disk
deva, deva-dcb-123t, 1.1.1.1, 128, 40
deva, deva-ncs-124t, 1.1.2.2, 64, 35
devb, abc-dcb-223t, 1.2.1.1, 46, 35
devb, devb-ncs-224t, 1.2.1.2, 46, 35
devb, devb-ncs-225t, 1.2.1.3, 64, 40
devc, devc-dcb-323t, 1.3.1.1, 64, 40
devc, devc-ncs-324t, 1.3.1.2, 32, 60
devd, devd-dcb-423t, 1.4.1.1, 32, 60
devd, devd-ncs-424t, 1.4.1.2, 32, 60
devd, devd-ncs-425t, 1.4.1.3, 32, 50

sheet_name="app_config"

env, name, Xmx, Xms, Xss
deva, deva-dcb-123t, 4096, 1024, 128
deva, deva-ncs-123t, 3218, 512, 64
devb, abc-dcb-123t, 2048, 128, 32
devb, devb-ncs-123t, 1024, 64, 32

"""

RETURN = """
  _raw:
    description:
      - value(s) stored in file column
    type: list
    elements: str
"""

display = Display()


class LookupModule(LookupBase):
    """ lookup module """

    def run(self, terms, variables=None, **kwargs):
        """ run method """
        self.set_options(var_options=variables, direct=kwargs)

        # populate options
        paramvals = self.get_options()
        display.v("parameters: " + str(paramvals))

        lookupfile = self.find_file_in_search_path(variables, 'files', paramvals['file'])

        if paramvals['join_type'] == 'cross' and paramvals['join_on'] is not None:
            raise ValueError("join_type: cross and Join_on are mutually exclusive")

        try:
            # Read data as strings from specified sheets
            dfs = []
            for sheet in paramvals['sheets']:
                df = pandas.read_excel(lookupfile, dtype='string',
                                       na_values=paramvals['default'],
                                       keep_default_na=False, sheet_name=sheet)

                # trim leading or trailing spaces
                if paramvals['trim']:
                    df = _whitespace_remover(df)
                dfs = dfs + [df]

            dataframe = dfs[0]
            for s in range(1, len(dfs), 1):
                dataframe = dataframe.merge(dfs[s], how=paramvals['join_type'],
                                            on=paramvals['join_on'])
                display.vvvv(f'Before filtering \n{dataframe}')

            if paramvals['cols']:
                _filter_columns(dataframe, paramvals['cols'] + [paramvals['filter_col']])

            if paramvals['filter'] and paramvals['filter_col']:
                if paramvals['filter_col'] not in dataframe.columns:
                    raise ValueError(
                        'filter_col: ' + paramvals['filter_col'] +
                        ' ,not found in ' + str(list(dataframe.columns))
                    )

                for x in dataframe.index:
                    if dataframe.loc[x, paramvals['filter_col']] != paramvals['filter']:
                        dataframe.drop(x, inplace=True)
            if len(dataframe) == 0:
                raise ValueError('no data rows left to return, review filters and source data')
            return dataframe.to_dict(orient='records')

        except (ValueError, AssertionError) as e:
            raise AnsibleError(e) from e


def _whitespace_remover(dataframe):
    dataframe = dataframe.rename(columns={v: v.strip() for v in dataframe.columns})

    # iterating over the columns
    for i in dataframe.columns:
        # checking datatype of each column
        if dataframe[i].dtype == 'string':

            # applying strip function on column
            dataframe[i] = dataframe[i].map(str.strip)
        else:
            # if condn. is False then it will do nothing.
            pass
    return dataframe


def _filter_columns(dataframe, cols):
    display.vvv(f'Column\'s found in datastream {list(dataframe.columns)}')
    if len(cols) >= 1:
        for h in dataframe.columns:
            if h not in cols:
                display.vvvvv(f'dropping column {h}')
                dataframe.drop(columns=h, inplace=True)

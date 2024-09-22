# (c) 2024, Mark Heynes <mark.heynes@heynesit.co.uk>
# (c) 2024 HeynesIT
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
""" excel_sheet.py """

from __future__ import annotations
from ansible.errors import AnsibleError
from ansible.plugins.lookup import LookupBase
from ansible.utils.display import Display
import pandas

DOCUMENTATION = r"""
    name: excel_sheet
    author: Mark Heynes <mark.heynes@heynesit.co.uk>
    short_description: read data from named sheet in XLSX file
    description:
      - The excel_sheet lookup reads the contents of a named sheet from an XLSX (Excel Open XML Spreadsheet) format file.
    options:
      sheet:
        description: sheet name to return
      cols:
        description: restrict columns returned to these + filter_col.
        default: []
        type: list
      filter_col:
        description: column to filter on.
        default: null
        type: string
      filter:
        description: text to filter data on.
        default: null
        type: string
      default:
        description: what to return if the cell is empty.
        default: 
      file:
        description: name of the XLSX file to open.
        default: null
        required: true
    notes:
      - if cols is not specified all columns are returned
"""

EXAMPLES = """
- name: msg="Match 'deva' on the 'env' column, but return the 'ip' column"
  ansible.builtin.debug: 
    msg="The ips in deva are {{ lookup('ansible.legacy.excel_sheet', file='sample.xlsx', sheet='infra', 
    filter='deva', filter_col='env', col='ip') }}"

# Contents of sample.xlsx
sheet_name="infra"

env   name           ip       ram         first_disk
deva  deva-dcb-123t  1.1.1.1  NaN         NaN
deva  deva-ncs-123t  1.1.2.2  NaN         NaN
devb   abc-dcb-123t  1.2.1.1  NaN         NaN
devb  devb-ncs-123t  1.2.1.2  NaN         NaN
devb  devb-ncs-123t  1.2.1.3  NaN         NaN
devc  devc-dcb-123t  1.3.1.1  NaN         NaN
devc  devc-ncs-123t  1.3.1.2  NaN         NaN
devd  devd-dcb-123t  1.4.1.1  NaN         NaN
devd  devd-ncs-123t  1.4.1.2  NaN         NaN
devd  devd-ncs-123t  1.4.1.3  NaN         NaN

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

        try:
            df = pandas.read_excel(lookupfile, dtype='string', na_values=paramvals['default'],
                                   keep_default_na=False, sheet_name=paramvals['sheet'])
            df = whitespace_remover(df)

            output_columns = paramvals['cols'] + [paramvals['filter_col']]
            if len(paramvals['cols']) >= 1:
                for h in df.columns:
                    if h not in output_columns:
                        df.drop(columns=h, inplace=True)
            if paramvals['filter'] and paramvals['filter_col']:
                if paramvals['filter_col'] not in df.columns:
                    raise ValueError('filter_col: ' + paramvals['filter_col'] + ' ,not found in ' + str(list(df.columns)))
                for x in df.index:
                    if df.loc[x, paramvals['filter_col']] != paramvals['filter']:
                        df.drop(x, inplace=True)
            if len(df) == 0:
                raise ValueError('no data rows left to return, review filters and source data')
            return df.to_dict(orient='records')

        except (ValueError, AssertionError) as e:
            raise AnsibleError(e)

def whitespace_remover(dataframe):
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


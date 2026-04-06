# Ansible Collection - `mutl3y.utils`

This collection provides lookup plugins for working with Excel (`.xlsx`) data in Ansible playbooks.

## Included plugins

- `mutl3y.utils.excel_sheet`
  - Read rows from a single named worksheet.
  - Supports optional row filtering by column value and optional column selection.
- `mutl3y.utils.excel_merge`
  - Read and merge rows from multiple worksheets.
  - Supports join type selection, row filtering, optional partial-match filtering, and column selection.

## Requirements

- `ansible-core` (set by your environment/runtime policy)
- Python dependencies required by the plugins:
  - `pandas`
  - Excel engine dependency such as `openpyxl` for `.xlsx` files

## Installation

Install from source (local checkout):

```bash
ansible-galaxy collection build /path/to/ansible_utils/utils
ansible-galaxy collection install /path/to/ansible_utils/utils/mutl3y-utils-*.tar.gz
```

## Usage examples

Lookup from a single sheet:

```yaml
- name: Read rows from one sheet
  ansible.builtin.debug:
    msg: >-
      {{ lookup(
          'mutl3y.utils.excel_sheet',
          file='sample.xlsx',
          sheet='infra',
          filter='deva',
          filter_col='env',
          cols=['ip']
      ) }}
```

Lookup and merge multiple sheets:

```yaml
- name: Merge rows across sheets
  ansible.builtin.debug:
    msg: >-
      {{ lookup(
          'mutl3y.utils.excel_merge',
          file='sample.xlsx',
          sheets=['infra', 'app_config'],
          join_on=['env', 'name'],
          join_type='left',
          filter='deva',
          filter_col='env'
      ) }}
```

## Notes

- Keep worksheet headers consistent and trimmed for reliable joins/filters.
- If filtering removes all rows, check source data and filter inputs.

# ansible_utils

Ansible utilities repository containing the `mutl3y.utils` collection.

## What this project provides

The collection currently includes Excel lookup plugins:

- `mutl3y.utils.excel_sheet`: read records from a single `.xlsx` worksheet.
- `mutl3y.utils.excel_merge`: read and merge records from multiple worksheets.

## Repository layout

- `galaxy.yml` - collection metadata
- `meta/runtime.yml` - collection runtime metadata
- `plugins/lookup/excel_sheet.py` - single-sheet lookup plugin
- `plugins/lookup/excel_merge.py` - multi-sheet merge lookup plugin

## Prerequisites

- Ansible installed (`ansible-core`)
- Python dependencies used by the lookup plugins:
  - `pandas`
  - `openpyxl` (for `.xlsx` support)

## Build and install the collection

```bash
cd ansible_utils
ansible-galaxy collection build .
ansible-galaxy collection install ./mutl3y-utils-*.tar.gz
```

## Use the plugins in playbooks

Use fully qualified collection names (FQCN), for example:

- `lookup('mutl3y.utils.excel_sheet', ...)`
- `lookup('mutl3y.utils.excel_merge', ...)`

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

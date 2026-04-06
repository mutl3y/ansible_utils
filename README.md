# ansible_utils

Ansible utilities repository containing the `mutl3y.utils` collection.

## What this project provides

The collection currently includes Excel lookup plugins:

- `mutl3y.utils.excel_sheet`: read records from a single `.xlsx` worksheet.
- `mutl3y.utils.excel_merge`: read and merge records from multiple worksheets.

Collection source is in:

- `./utils`

## Repository layout

- `utils/galaxy.yml` - collection metadata
- `utils/meta/runtime.yml` - collection runtime metadata
- `utils/plugins/lookup/excel_sheet.py` - single-sheet lookup plugin
- `utils/plugins/lookup/excel_merge.py` - multi-sheet merge lookup plugin

## Prerequisites

- Ansible installed (`ansible-core`)
- Python dependencies used by the lookup plugins:
  - `pandas`
  - `openpyxl` (for `.xlsx` support)

## Build and install the collection

```bash
cd <repository-root>
ansible-galaxy collection build ./utils
ansible-galaxy collection install ./utils/mutl3y-utils-*.tar.gz
```

## Use the plugins in playbooks

Use fully qualified collection names (FQCN), for example:

- `lookup('mutl3y.utils.excel_sheet', ...)`
- `lookup('mutl3y.utils.excel_merge', ...)`

See `./utils/README.md` for detailed plugin examples.

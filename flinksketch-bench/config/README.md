# Configuration Files

## Template vs Local Configs

Templates are stored in `config/templates/` with placeholders. Local configs are gitignored.

**Structure:**
```
config/
├── templates/                         # Templates (committed to repo)
│   ├── experiment_config.yaml
│   └── aggregations/
│       ├── baseline/
│       ├── datasketches/
│       ├── custom/
│       └── ddsketch/
├── experiment_config.yaml             # Your local config (gitignored)
└── aggregations/                      # Your local configs (gitignored)
    ├── baseline/
    ├── datasketches/
    ├── custom/
    └── ddsketch/
```

## Setup

```bash
# Copy templates to create your local configs
cp config/templates/experiment_config.yaml config/experiment_config.yaml
cp -r config/templates/aggregations/* config/aggregations/

# Replace <PROJECT_ROOT> with your actual project path
sed -i 's|<PROJECT_ROOT>|/your/actual/path|g' config/experiment_config.yaml
sed -i 's|<PROJECT_ROOT>|/your/actual/path|g' config/aggregations/**/*.yaml

# Fill in empty values in the configs
# Use the local configs in your scripts
python run_experiment.py --config config/experiment_config.yaml
```

## Rules

- ✅ **Commit**: `config/templates/**/*.yaml` files with placeholders
- ❌ **Don't commit**: Config files outside `templates/` folder (automatically gitignored)
- 📝 **Update**: When adding new options, update files in `config/templates/`

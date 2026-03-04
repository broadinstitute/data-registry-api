# CalR - Calorimetry Data Conversion (Python Port)

Python port of the R CalR conversion logic for converting raw calorimetry data from various manufacturers to a standardized format.

## Supported Formats

- **Oxymax/CLAMS** (Columbus Instruments)
- **TSE Systems** (LabMaster)
- **Sable Systems** (Macro 13)
- **CalR** (standardized format)

## Installation

```bash
# From the data-registry-api directory
pip install -e ./calr
```

## Usage

```python
from calr import load_cal_file

# Automatically detects format and converts
df = load_cal_file('path/to/calorimetry_data.csv')

# Result is a pandas DataFrame in standard CalR format
print(df.columns)
print(df.head())
```

## Development Status

**Current Status**: Initial structure created

- [x] Package structure
- [x] Format detection
- [ ] TSE loader/converter (in progress)
- [ ] Oxymax loader/converter
- [ ] Sable loader/converter  
- [ ] CalR retrofit logic
- [ ] Comprehensive tests

## Testing

```bash
# Run tests
pytest calr/tests/ -v

# Run with coverage
pytest calr/tests/ --cov=calr --cov-report=html
```

## Test Data

Test files are located in `calr/test_data/`:
- `calr_tse.csv` - TSE LabMaster format
- `calr_test_data.csv` - Standard CalR format

## Standard CalR Format

All converters produce DataFrames with these columns:

**Identifiers:**
- `subject.id`, `subject.mass`, `cage`

**Time:**
- `Date.Time`, `minute`, `hour`, `day`
- `exp.minute`, `exp.hour`, `exp.day`

**Metabolic:**
- `vo2`, `vco2`, `ee`, `ee.acc`, `rer`

**Behavioral:**
- `feed`, `feed.acc`, `drink`, `drink.acc`
- `xytot`, `xyamb`, `wheel`, `wheel.acc`
- `pedmeter`, `allmeter`, `body.temp`

## Comparison with R Implementation

To validate parity with the R implementation, use:

```bash
# Run R conversion
Rscript calr/tests/run_r_conversion.R

# Run Python conversion
python calr/tests/compare_r_python.py
```

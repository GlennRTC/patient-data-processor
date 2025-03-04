# Patient Data Consolidation Tool

## Project Overview
This Python script consolidates patient laboratory data from a CSV file, aggregating multiple entries for the same patient and collection date into a single row.

## Features
- Processes large CSV files with multiple rows per patient
- Handles mixed data types
- Chunks output into multiple files for easier management
- Robust error handling for data type conversions

## Prerequisites
- Python 3.8+
- pandas library
- numpy library

## Installation

### 1. Clone the Repository
```bash
git clone <your-repository-url>
cd patient-data-consolidation
```

### 2. Create Virtual Environment (Recommended)
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
```

### 3. Install Dependencies
```bash
pip install pandas numpy
```

## Usage

### Run the Script
```bash
python consolidate_patient_data.py
```

### Customization
- Modify `input_file` in the script to process different CSV files
- Adjust `chunk_size` parameter to control output file sizes
- Customize `output_dir` to specify output location

## Input Requirements
- CSV file with patient data
- Columns should include:
  - Patient
  - CollectDate
  - Various measurement columns

## Output
- Multiple CSV files in the specified output directory
- `chunk_summary.txt` with processing details

## Handling Large Files
- Script is optimized for files with hundreds of thousands of rows
- Uses memory-efficient processing techniques

## Troubleshooting
- Ensure all required libraries are installed
- Check input file format and column names
- Verify Python version compatibility

# AEMO Data Replicator

This project replicates AEMO data from SQLLoader archives and NEMWeb Current/Archive directories.

## Architecture

- **Batcher**: Downloads zip files from AEMO sites and extracts them (handling nested zips) to a directory.
- **Loader**: Scans the directory for CSV files, determines the appropriate database table, and loads the data.

## Sources

1. **Current Data**: https://nemweb.com.au/Reports/Current/ - Latest data with subdirs containing zips.
2. **Archive**: https://nemweb.com.au/Reports/ARCHIVE/ - Archived data similar to Current.
3. **Data Model Archive**: https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/YYYY/MMSDM_YYYY_MM/MMSDM_Historical_Data_SQLLoader/DATA/ - Historical data in monthly packages, with zips for specific tables.

## Tables

The following tables are targeted for download and loading:
- DISPATCHPRICE
- DISPATCHLOAD
- DISPATCHREGIONSUM
- TRADINGPRICE
- TRADINGREGIONSUM
- DUDETAIL
- DUDETAILSUMMARY
- DISPATCHINTERCONNECTORRES
- TRADINGINTERCONNECT
- TRANSMISSIONLOSSFACTOR

## Setup

1. Install dependencies: `pip install -r requirements.txt`
2. Configure `config/config.yaml` with database credentials and AEMO site URLs.
3. Run the replicator: `python main.py`

## Usage

- Run both batcher and loader: `python main.py`
- Batcher only: `python main.py --batcher` or `python scripts/run_batcher.py`
- Loader only: `python main.py --loader` or `python scripts/run_loader.py`

Currently, the batcher focuses on scraping the Data Model Archive (source 3) for historical data from 2016-2025.

## Project Structure

- `batcher/`: Batcher modules
- `loader/`: Loader modules
- `database/`: Database connection and models
- `utils/`: Utilities like logging
- `config/`: Configuration files
- `data/`: Data directories
- `logs/`: Log files
- `scripts/`: Standalone scripts
- `tests/`: Unit tests
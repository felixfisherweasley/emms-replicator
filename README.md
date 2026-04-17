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

- One-shot orchestration (legacy convenience): `python main.py --source mmsdm`
- One-shot batcher by source:
	- `python scripts/run_batcher_mmsdm.py`
	- `python scripts/run_batcher_archive.py`
	- `python scripts/run_batcher.py --source mmsdm`
	- `python scripts/run_batcher.py --source archive`
- Continuous current-source batcher poller:
	- `python scripts/run_batcher_current.py`
	- `python scripts/run_batcher_current.py --interval-seconds 120`
- Loader:
	- One-shot: `python scripts/run_loader.py`
	- Continuous service: `python scripts/run_loader_service.py`
	- Continuous service custom interval: `python scripts/run_loader_service.py --interval-seconds 15`

Notes:
- Sources are intentionally run one-at-a-time so they can be scheduled at different frequencies.
- `mmsdm` and `archive` are backfill-oriented.
- `current` is intended for live polling against configured `current_feeds` URLs.

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
# Boligsok

A tool for tracking Oslo apartment listings from Finn.no. It geocodes addresses, merges new listings with existing data, and uploads to Atlas for visualization.

## Setup

1. Create a virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. Copy `.env.template` to `.env` and fill in your credentials:
```bash
cp .env.template .env
```

## Configuration

| Variable | Description |
|----------|-------------|
| `ATLAS_USERNAME` | Username for Atlas |
| `ATLAS_PASSWORD` | Password for Atlas |
| `DATASET_ID` | Dataset ID to download existing data from (optional for first run) |
| `WEBHOOK_URL` | Webhook URL for uploading data to Atlas |
| `DOWNLOAD_PATH` | Path to your downloads folder |
| `PATH_ROOT` | Path to the root of this project |
| `DOWNLOAD_FILE_NAME` | Name of the CSV file from the webscraper |

## Usage

1. Export apartment listings from Finn.no as a CSV file
2. Run the script:
```bash
python automatic_upload.py
```

The script will:
- Move the CSV from your downloads folder (if present)
- Geocode new addresses using Nominatim (~1 second per address)
- Merge with existing data from Atlas (if `DATASET_ID` is set)
- Upload the merged data via webhook

## Tip

Add an alias to your shell config (`.bashrc`, `.zshrc`, etc.) for quick access:
```bash
alias boligsok="cd /path/to/boligsok && source venv/bin/activate && python automatic_upload.py"
```

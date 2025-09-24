# Instructions to Run the PDF Downloader Script

1. Install GeckoDriver:
   - Download: `https://github.com/mozilla/geckodriver/releases'
        - Download appropriate version for v0.36
        - Unzip
   - Move to PATH: `sudo mv geckodriver /usr/local/bin/`
   - Make executable: `sudo chmod +x /usr/local/bin/geckodriver`
   - Verify: `geckodriver --version`

2. Usage:
   - Ensure `well_log_links.csv` has "Api Num" and "Document Access" columns.
   - Run in PyCharm with active venv: `python scrape_and_download_pdfs.py`
   - PDFs are saved to:
     - `downloaded_pdfs/well_logs/` for WELL_LOG files
     - `downloaded_pdfs/well_file_historic/` for WELL_FILE_HISTORIC files
     - `downloaded_pdfs/other_files/<well_api>/` for others
   - Logs saved to `scrape_and_download_pdfs.log`


Example:
well_log_links.csv : list of links
downloaded_pdfs : resulting folder with saved pdfs
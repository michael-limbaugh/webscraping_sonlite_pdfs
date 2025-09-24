import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
import logging
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        #logging.FileHandler('scrape_and_download_pdfs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Local folder to save PDFs
base_save_folder = "downloaded_pdfs"
well_log_folder = os.path.join(base_save_folder, "well_logs")
well_file_historic_folder = os.path.join(base_save_folder, "well_file_historic")
other_files_folder = os.path.join(base_save_folder, "other_files")
os.makedirs(well_log_folder, exist_ok=True)
os.makedirs(well_file_historic_folder, exist_ok=True)
os.makedirs(other_files_folder, exist_ok=True)

# Input CSV filepath
csv_filepath = "well_log_links.csv"
link_column_name = 'Document Access'
api_column_name = 'Api Num'

# Read URLs and API numbers from CSV
try:
    df = pd.read_csv(csv_filepath)
    if link_column_name not in df.columns or api_column_name not in df.columns:
        raise KeyError(f"CSV must contain '{link_column_name}' and '{api_column_name}' columns")
    urls = df[[link_column_name, api_column_name]].dropna().to_dict('records')  # List of {link, api}
except FileNotFoundError:
    logger.error(f"CSV file {csv_filepath} not found.")
    exit()
except KeyError as e:
    logger.error(f"CSV file error: {e}")
    exit()
except Exception as e:
    logger.error(f"Error reading CSV file: {e}")
    exit()

# Print total number of URLs
total_urls = len(urls)
logger.info(f"Total URLs to process: {total_urls}")

# Set up a session for downloading files
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:143.0) Gecko/20100101 Firefox/143.0'
})

#successful_downloads = 0
failed_urls = []
lock = threading.Lock()  # For thread-safe logging and counters
max_pages = 25  # Safety limit for pagination

# Set up single WebDriver instance
options = Options()
options.add_argument('-headless')  # Uncomment for headless mode after debugging
options.add_argument('--no-sandbox')
options.add_argument('--disable-gpu')
options.add_argument('--window-size=1920,1080')
options.add_argument('--disable-extensions')
options.add_argument('--disable-background-networking')
options.add_argument('--disable-sync')
options.add_argument('--disable-sandbox')
options.binary_location = '/snap/firefox/6836/usr/lib/firefox/firefox'

logger.info("Initializing GeckoDriver")
service = Service('/usr/local/bin/geckodriver')
driver = webdriver.Firefox(service=service, options=options)


def download_file(data, file_index, total_files):
    """Download a single file with thread-safe logging"""
    global successful_downloads
    well_api = data['well_api']
    document_type = data['document_type']
    content_id = data['content_id']
    link = data['link']

    # Save with content_id to avoid race conditions
    is_well_log = 'WELL_LOG' in document_type.upper()
    is_historic = 'WELL_FILE_HISTORIC' in document_type.upper()
    if is_well_log:
        save_folder = well_log_folder
    elif is_historic:
        save_folder = well_file_historic_folder
    else:
        save_folder = os.path.join(other_files_folder, well_api)
    filename = f"{well_api}_{document_type}_{content_id}.pdf"
    filepath = os.path.join(save_folder, filename)

    try:
        # Check if file already exists
        if os.path.exists(filepath):
            logger.info(f"[{file_index}/{total_files}] File {filename} already exists, skipping...")
            with lock:
                successful_downloads += 1
            return True

        # Resolve relative URLs
        if not link.startswith('http'):
            base_url = 'https://sonlite.dnr.state.la.us'
            link = base_url + link
            logger.info(f"[{file_index}/{total_files}] Resolved relative URL to: {link}")

        # Download the file
        #logger.info(f"[{file_index}/{total_files}] Downloading PDF to {save_folder}")
        response = session.get(link, stream=True, timeout=30)
        response.raise_for_status()

        # Verify content type
        content_type = response.headers.get('content-type', '')
        if 'application/pdf' not in content_type.lower():
            logger.error(
                f"[{file_index}/{total_files}] Link did not return expected PDF (Content-Type: {content_type})")
            return False

        # Save the file
        os.makedirs(save_folder, exist_ok=True)
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logger.info(f"[{file_index}/{total_files}] Downloaded {filename} to {save_folder}")
        with lock:
            successful_downloads += 1
        return True

    except requests.RequestException as e:
        logger.error(f"[{file_index}/{total_files}] Error downloading {link}: {e}")
        return False
    except Exception as e:
        logger.error(f"[{file_index}/{total_files}] Error saving {filename}: {e}")
        return False


def rename_files(well_api, pdf_data):
    """Rename files to use sequential counters per document_type"""
    file_counters = {}  # Per well_api document_type counters
    for data in pdf_data:
        document_type = data['document_type']
        content_id = data['content_id']
        is_well_log = 'WELL_LOG' in document_type.upper()
        is_historic = 'WELL_FILE_HISTORIC' in document_type.upper()
        if is_well_log:
            save_folder = well_log_folder
        elif is_historic:
            save_folder = well_file_historic_folder
        else:
            save_folder = os.path.join(other_files_folder, well_api)

        old_filename = f"{well_api}_{document_type}_{content_id}.pdf"
        old_filepath = os.path.join(save_folder, old_filename)

        if document_type not in file_counters:
            file_counters[document_type] = 1
        new_filename = f"{well_api}_{document_type}_{file_counters[document_type]}.pdf"
        new_filepath = os.path.join(save_folder, new_filename)

        try:
            if os.path.exists(old_filepath):
                os.rename(old_filepath, new_filepath)
                logger.info(f"Renamed {old_filename} to {new_filename} in {save_folder}")
                file_counters[document_type] += 1
        except Exception as e:
            logger.error(f"Error renaming {old_filename} to {new_filename}: {e}")


# Process each URL
for i, entry in enumerate(urls, 1):

    url = entry[link_column_name]
    well_api = str(entry[api_column_name]).strip()  # Use Api Num from CSV
    logger.info(f"Processing Well API {'.' * i} ({i}/{total_urls})")
    logger.info(f"Processing URL {'.' * i} ({i}/{total_urls})")
    pdf_data = []
    processed_content_ids = set()  # Track unique content IDs per URL
    successful_downloads = 0
    total_files = 0
    file_index = 0  # Track current file index for logging
    previous_content_ids = set()

    try:
        logger.info("Navigating to URL")
        driver.get(url)

        # Wait for page body
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        logger.info("Page body loaded")

        # Loop through pages
        page_number = 1
        while page_number <= max_pages:
            logger.info(f"Processing page {page_number}")
            # Wait for the table
            table_found = False
            try:
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "a-IRR-table"))
                )
                logger.info("Table found")
                table_found = True
            except TimeoutException:
                logger.warning("Timeout for table, trying alternative selectors")
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "table"))
                    )
                    logger.info("Found table element (generic selector)")
                    table_found = True
                except TimeoutException:
                    logger.error(f"No table found on page {page_number}")
                    failed_urls.append((url, f"No table found on page {page_number}"))
                    break

            if not table_found:
                break

            # Parse the page
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            table = soup.find('table', class_='a-IRR-table')
            if not table:
                table = soup.find('table')
                if not table:
                    logger.error(f"No table found in parsed HTML on page {page_number}")
                    break
                logger.info("Using fallback table selector")

            # Extract rows
            rows = table.find_all('tr')[1:]
            logger.info(f"Found {len(rows)} rows on page {page_number}")
            total_files += len(rows)  # One file per row (PDF only)
            current_content_ids = set()

            for row in rows:
                row_data = {
                    'well_api': well_api,
                    'document_type': 'unknown',
                    'content_id': 'unknown',
                    'link': None
                }

                # Extract document_type
                doc_type_cell = row.find('td', headers='C278166632217359383')
                if doc_type_cell:
                    row_data['document_type'] = doc_type_cell.text.strip()
                    row_data['document_type'] = re.sub(r'[^\w\-]', '_', row_data['document_type'])

                # Extract content_id
                content_id_cell = row.find('td', headers='docname')
                if content_id_cell:
                    row_data['content_id'] = content_id_cell.text.strip()

                # Extract first link (PDF)
                docview_cell = row.find('td', headers='docview')
                if docview_cell:
                    link_tags = docview_cell.find_all('a')
                    if link_tags and 'dDocname' in link_tags[0].get('href', ''):
                        row_data['link'] = link_tags[0]['href']

                if row_data['content_id'] != 'unknown':
                    current_content_ids.add(row_data['content_id'])
                if row_data['link'] and row_data['content_id'] not in processed_content_ids:
                    pdf_data.append(row_data)
                    processed_content_ids.add(row_data['content_id'])

            # Check for duplicate content IDs
            if current_content_ids == previous_content_ids and page_number > 1:
                logger.warning(f"Page {page_number} has same content IDs as previous page, stopping pagination")
                break
            previous_content_ids = current_content_ids

            # Check for next button
            if page_number < max_pages:
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR,
                                                      'div.a-IRR-paginationWrap button.a-IRR-button--pagination[title="Next"]')
                    parent_li = next_button.find_element(By.XPATH, '..')
                    if 'is-disabled' in parent_li.get_attribute('class'):
                        logger.info("No more pages to process")
                        break
                    logger.info("Clicking Next button")
                    for attempt in range(3):
                        try:
                            next_button.click()
                            time.sleep(2)  # Wait for page to load
                            WebDriverWait(driver, 60).until(
                                EC.presence_of_element_located((By.CLASS_NAME, "a-IRR-table"))
                            )
                            new_soup = BeautifulSoup(driver.page_source, 'html.parser')
                            new_table = new_soup.find('table', class_='a-IRR-table')
                            if new_table:
                                new_rows = new_table.find_all('tr')[1:]
                                new_content_ids = {row.find('td', headers='docname').text.strip() for row in new_rows if
                                                   row.find('td', headers='docname')}
                                if new_content_ids == current_content_ids:
                                    logger.warning(
                                        f"Page {page_number + 1} has same content as page {page_number}, stopping pagination")
                                    break
                            break
                        except (TimeoutException, StaleElementReferenceException) as e:
                            logger.warning(f"Retry {attempt + 1}/3: Error clicking Next button: {e}")
                            time.sleep(2)
                            next_button = driver.find_element(By.CSS_SELECTOR,
                                                              'div.a-IRR-paginationWrap button.a-IRR-button--pagination[title="Next"]')
                    else:
                        logger.error(f"Failed to click Next button after 3 retries")
                        break
                    page_number += 1
                except (NoSuchElementException, StaleElementReferenceException):
                    logger.info("No more pages to process")
                    break
                except Exception as e:
                    logger.error(f"Error clicking Next button: {e}")
                    break
            else:
                logger.info(f"Reached maximum page limit ({max_pages})")
                break

    except Exception as e:
        logger.error(f"Error processing Well API {well_api}: {e}")
        failed_urls.append((url, f"Error during processing: {str(e)}"))
        logger.info(f"Completed Well API {'.' * i} ({i}/{total_urls})")
        continue

    # Download files in parallel
    if pdf_data:
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_file = {executor.submit(download_file, data, i + file_index + 1, total_files): data for i, data in
                              enumerate(pdf_data)}
            for future in as_completed(future_to_file):
                future.result()  # Wait for completion
        file_index += len(pdf_data)

    # Rename files after downloads
    rename_files(well_api, pdf_data)

    logger.info(f"Successful downloads: {successful_downloads}/{total_files} for Well API {well_api}")
    logger.info(f"Completed Well API {'.' * i} ({i}/{total_urls})")

# Close WebDriver and session
try:
    driver.quit()
    logger.info("WebDriver closed")
except Exception as e:
    logger.warning(f"Error closing WebDriver: {e}")
session.close()

# Print final summary
logger.info(f"\nDownload process completed.")
if failed_urls:
    logger.info("\nURLs that failed or had no files:")
    for url, reason in failed_urls:
        logger.info(f"{reason}")
else:
    logger.info("No URLs failed or had no files.")
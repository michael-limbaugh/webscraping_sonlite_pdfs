import requests
from bs4 import BeautifulSoup
import os
import urllib.parse
import csv
import pandas as pd

# Local folder to save PDFs
save_folder = "downloaded_pdfs"
os.makedirs(save_folder, exist_ok=True)

# Input CSV filepath (modify this to your CSV file path)
csv_filepath = "well_log_links.csv"  # Replace with the actual path to your CSV file

link_column_name = 'Document Access'

# Read URLs from CSV
try:
    df = pd.read_csv(csv_filepath)
    # Assuming URLs are in a column named 'url' (adjust if column name differs)
    urls = df[link_column_name].dropna().tolist()  # Drop any NaN values
except FileNotFoundError:
    print(f"Error: CSV file {csv_filepath} not found.")
    exit()
except KeyError:
    print(f"Error: CSV file must contain a '{link_column_name}' column.")
    exit()
except Exception as e:
    print(f"Error reading CSV file: {e}")
    exit()

# Print total number of links
total_urls = len(urls)
print(f"Total URLs to process: {total_urls}")

# Set up a session to handle potential cookies or authentication
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
})

successful_downloads = 0
failed_urls = []

# Process each URL
for i, url in enumerate(urls, 1):
    print(f"\nProcessing URL {i}/{total_urls}: {url}")

    # Fetch the webpage
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()  # Check for HTTP errors
        html_content = response.text
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        failed_urls.append((url, "Could not be reached"))
        print(f"Completed {i}/{total_urls}")
        continue

    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find the table containing document results
    table = soup.find('table', class_='a-IRR-table')
    pdf_data = []

    if table:
        # Find all rows in the table, skipping the header
        rows = table.find_all('tr')[1:]  # Skip header row

        # Extract PDF links, well_api, and document_type
        for row in rows:
            docview_cell = row.find('td', headers='docview')
            well_api_cell = row.find('td', headers='C278297701557366271')  # Well Serial Num
            document_type_cell = row.find('td', headers='C278166632217359383')  # Document Type

            # Initialize data for this row
            row_data = {
                'well_api': well_api_cell.text.strip() if well_api_cell else 'unknown',
                'document_type': document_type_cell.text.strip().replace(' ', '_') if document_type_cell else 'unknown',
                'link': None
            }

            # Prefer docview link for the PDF
            if docview_cell and docview_cell.find('a'):
                docview_link = docview_cell.find('a')['href']
                if 'dDocname' in docview_link:
                    row_data['link'] = docview_link

            if row_data['link']:  # Only add rows with a valid link
                pdf_data.append(row_data)

    # Check if any PDFs were found
    if not pdf_data:
        print(f"No PDFs found for {url}")
        failed_urls.append((url, "No PDFs found"))
        print(f"Completed {i}/{total_urls}")
        continue

    # Download one PDF per row
    for data in pdf_data:
        well_api = data['well_api']
        document_type = data['document_type']
        link = data['link']
        filename = f"{well_api}_{document_type}.pdf"
        filepath = os.path.join(save_folder, filename)

        try:
            # Check if file already exists
            if os.path.exists(filepath):
                print(f"File {filename} already exists, skipping...")
                successful_downloads += 1
                continue

            # Download the PDF
            pdf_response = session.get(link, stream=True, timeout=30)
            pdf_response.raise_for_status()

            # Save the PDF to the local folder
            with open(filepath, 'wb') as f:
                for chunk in pdf_response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            print(f"Downloaded {filename} to {save_folder}")
            successful_downloads += 1

        except requests.RequestException as e:
            print(f"Error downloading {link}: {e}")
        except Exception as e:
            print(f"Error saving {filename}: {e}")

    print(f"Completed {i}/{total_urls}")

# Close the session
session.close()

# Print summary
print(f"\nDownload process completed.")
print(f"Successful downloads: {successful_downloads}/{total_urls}")
if failed_urls:
    print("\nURLs that failed or had no PDFs:")
    for url, reason in failed_urls:
        print(f"{url}: {reason}")
else:
    print("No URLs failed or had no PDFs.")
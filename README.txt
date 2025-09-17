This program:
1) Uses an input csv file (named csv_filepath), saved locally in the project directory
2) Reads the list of hyperlinks to sonlite based on the column name (link_column_name)
3) Iterates through the list and scrapes the websites for all pdfs
4) Saves pdfs according to Well API name and the document type in a local folder
5) Logs periodic progress

To use:
1) Install required libraries
2) update local csv (mine is named 'well_log_links.csv')
3) Run the python code (webscrape.py)

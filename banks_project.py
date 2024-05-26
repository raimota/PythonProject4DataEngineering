import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime
import os


def downloadExchangeRate():
    """
    This function downloads the exchange rate data from an online source.
    If the file 'exchange_rate.csv' already exists in the current directory, it will be removed.
    After removing the existing file, the function logs the start of the data download process, downloads the file 'exchange_rate.csv' from the specified URL using the 'wget' command, and logs a success message once the download is complete.
    """

    if os.path.exists("exchange_rate.csv"): os.remove("exchange_rate.csv")
    log_progress("Starting data download: 'exchange_rate.csv'...")

    url_exchange_rate = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
    
    os.system(f"wget {url_exchange_rate}")
    log_progress("'exchange_rate.csv' successfully downloaded")

def extract(url, table_attribs):

    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    log_progress("Extracting data...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        table = soup.find_all('table')[0]
        data = []

        for row in table.find_all('tr')[1:]:
            cols = row.find_all('td')
            if cols:
                country = cols[1].text.strip()
                marketCap = cols[2].text.strip().replace(',', '')
                data.append([country, marketCap])

        df = pd.DataFrame(data, columns=table_attribs)
        log_progress("Data extracted")
        return df
        
    except Exception as e:
        log_progress(f"Error extracting data: {e}")

def transform(df):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    
    try:
        log_progress("Transforming data...")

        # Read the CSV file into a DataFrame
        dataframe = pd.read_csv('exchange_rate.csv')

        # Create a dictionary using the DataFrame
        exchange_rates = dataframe.set_index('Currency').to_dict()['Rate']

        df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float).round(2)

        # Convert MC_USD_Billion para GBP, EUR e INR
        df['MC_GBP_Billion'] = [np.round(x*exchange_rates['GBP'],2) for x in df['MC_USD_Billion']]
        df['MC_EUR_Billion'] = [np.round(x*exchange_rates['EUR'],2) for x in df['MC_USD_Billion']]
        df['MC_INR_Billion'] = [np.round(x*exchange_rates['INR'],2) for x in df['MC_USD_Billion']]

        log_progress("Data successfully transformed")
        return df

    except Exception as e:
        log_progress(f"Error while transforming data: {e}")

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

    try:
        df.to_csv(csv_path, index=False)
        log_progress(f"Data saved to CSV file: {csv_path}")
    except Exception as e:
        log_progress(f"Error saving data to CSV file: {e}")

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''

    try:
        log_progress("Saving data to database...")
        conn = sqlite3.connect(sql_connection)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        log_progress(f"Data saved to database table: {table_name}")
    except Exception as e:
        log_progress(f"Error saving data to database table: {e}")

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    try:
        conn = sqlite3.connect(sql_connection)
        result = pd.read_sql_query(query_statement, conn)
        print(result)
        conn.close()
        log_progress(f"SQL query executed successfully: {query_statement}")
    except Exception as e:
        log_progress(f"Error running query: {e}")

def log_progress(message): 
    timestamp_format = '%Y-%m-%d-%H:%M:%S' # Year-Month-Day-Hour-Minute-Second 
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

# Initialize entities
url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
sql_connection = 'Banks.db'

# Download exchange_rate.csv
downloadExchangeRate()

# Extract data from the website
df = extract(url, table_attribs)
print(df)

# Transform the data
df = transform(df)
print("\n\n")
print(df)

# Load data to CSV
load_to_csv(df, csv_path)

# Load data to database
load_to_db(df, sql_connection, table_name)

# Run query on the database
query_statement = "SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)

# Run query on the database
query_statement = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)

# Run query on the database
query_statement = "SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)


log_progress("ETL process completed successfully.")
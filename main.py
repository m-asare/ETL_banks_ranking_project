"""
You have been hired as a data engineer by research organization. Your boss has asked you to create a code that can be
used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD.
Further, the data needs to be transformed and stored in GBP, EUR and INR as well, in accordance with the exchange rate
information that has been made available to you as a CSV file. The processed information table is to be saved locally
in a CSV format and as a database table. Your job is to create an automated system to generate this information so
that the same can be executed in every financial quarter to prepare the report
"""


import sqlite3
from datetime import datetime
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

URL = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
tb_name = 'Largest_banks'
csv_path = 'exchange_rate.csv'
output_path_csv = 'Largest_banks_data.csv'


def log_progress(message):
    """This function logs the mentioned message of a given stage of the
        code execution to a log file."""
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a", encoding="utf-8") as file:
        file.write(timestamp + ' : ' + message + '\n')


def extract(url):
    """This function extracts the required information from the website and save it to a DataFrame"""
    page = requests.get(url, timeout=10).text
    soup = BeautifulSoup(page, 'html.parser')
    tables = soup.find_all('table')
    dfs = pd.read_html(str(tables[0]), flavor='bs4')
    dataset = dfs[0].drop(columns=['Rank'], errors='ignore')
    dataset = dataset.rename(columns={'Bank name': 'Name', 'Market cap (US$ billion)': 'MC_USD billion'})

    return dataset


def transform(dataset, csv_file):
    """This function accesses the CSV file for exchange rate
    information, and adds three columns to the DataFrame, each
    containing the transformed version of Market Cap column to
    respective currencies"""
    with open(csv_file, 'r', encoding="utf-8") as file:
        exchange_rate = pd.read_csv(file)

    rates = exchange_rate['Rate']
    rates_list = [float(x) for x in rates]

    market_cap = dataset['MC_USD billion'].to_list()
    float_market_cap = [float(i) for i in market_cap]
    dataset["MC_EUR_Billion"] = [np.round(float(a) * rates_list[0], 3) for a in float_market_cap]
    dataset["MC_GBP_Billion"] = [np.round(float(a) * rates_list[1], 3) for a in float_market_cap]
    dataset["MC_INR_Billion"] = [np.round(float(a) * rates_list[2], 3) for a in float_market_cap]

    return dataset


def load_to_csv(dataset, csv_output):
    """This function saves the final DataFrame as a CSV file."""
    dataset.to_csv(csv_output)


def load_to_db(dataset, sql_connection, table_name):
    """This function saves the final DataFrame to a table in the database"""
    dataset.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    """This function queries the database"""
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


log_progress('Preliminaries complete. Initializing ETL process')
df = extract(URL)
log_progress('Data extraction complete. Initializing Transformation process')
df = transform(df, csv_path)
log_progress('Data transformation complete. Initializing Loading process')
load_to_csv(df, output_path_csv)
log_progress('Data saved to csv file')
sql_conn = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_conn, tb_name)
log_progress('Data loaded to Database as table. Running the query')
sql_statement = f"SELECT * from {tb_name}"
run_query(sql_statement, sql_conn)
log_progress('Process Complete.')
sql_conn.close()

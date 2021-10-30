import argparse
import pandas
import os
import time
import yfinance as yf

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest import DataFormat, IngestionProperties, QueuedIngestClient
from datetime import datetime as dt
from dotenv import load_dotenv
from typing import Tuple

def initializeKustoClients() -> Tuple[KustoClient, QueuedIngestClient]:
    aad_tenant_id = os.getenv("AAD_TENANT_ID")
    kusto_uri = os.getenv("KUSTO_URI")
    kusto_ingest_uri = os.getenv("KUSTO_INGEST_URI")

    ingest_connection_string = KustoConnectionStringBuilder.with_aad_device_authentication(kusto_ingest_uri, aad_tenant_id)
    connection_string = KustoConnectionStringBuilder.with_aad_device_authentication(kusto_uri, aad_tenant_id)

    client = KustoClient(connection_string)
    ingest_client = QueuedIngestClient(ingest_connection_string)

    return client, ingest_client

def createTableForTickers(tickers: list, client: KustoClient, database_name: str) -> None:
    print("Creating table for tickers")
    for ticker in tickers:
        print(f"Creating table for {ticker}")
        create_table_command = f".create table {ticker} (Time: datetime, Price: real)"
        client.execute_mgmt(database_name, create_table_command)
        print(f"Table for {ticker} created")

def uploadData(table_name: str, df: pandas.DataFrame, client: QueuedIngestClient, database_name: str) -> None:
    print(f"Queing data upload to table {table_name}")
    ingestion_props = IngestionProperties(database=database_name, table=table_name, data_format=DataFormat.CSV)
    client.ingest_from_dataframe(df, ingestion_properties=ingestion_props)

def getStockPrices(tickers: str, sleep_time: int, client: KustoClient, ingest_client: QueuedIngestClient, database_name: str) -> None:
    tickers_arr = tickers.split(',')
    createTableForTickers(tickers_arr, client, database_name)

    fields = ["Time", "Price"]
    while True:
        for ticker in tickers_arr:
            currentTime = dt.now()
            tkr = yf.Ticker(ticker)
            print(f"{currentTime}: Ticker {ticker} is currently: {tkr.info['regularMarketPrice']}")

            rows = [[currentTime, tkr.info['regularMarketPrice']]]
            df = pandas.DataFrame(data=rows, columns=fields)
            uploadData(ticker, df, ingest_client, database_name)

        time.sleep(sleep_time)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Print out stock prices for different stocks')
    parser.add_argument("-t", "--tickers", default="msft",
                    help="What tickers do you want to see values for. Separated by commas")
    parser.add_argument("-s", "--sleep", type=int, default=10,
                    help="How many seconds to sleep for between iterations")
    args = parser.parse_args()

    load_dotenv()
    client, ingest_client = initializeKustoClients()

    database_name = os.getenv("KUSTO_DATABASE")
    getStockPrices(args.tickers, args.sleep, client, ingest_client, database_name)
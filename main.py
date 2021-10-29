# Raw Package
import argparse
import time
import yfinance as yf

from datetime import datetime as dt

def getStockPrices(tickers: str, sleep_time: int):
    while True:
        currentTime = dt.now()

        tickers_arr = tickers.split(',')
        for ticker in tickers_arr:
            tkr = yf.Ticker(ticker)
            print(f"{currentTime}: Ticker {ticker} is currently: {tkr.info['regularMarketPrice']}")

        time.sleep(sleep_time)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Print out stock prices for different stocks')
    parser.add_argument("-t", "--tickers", default="msft",
                    help="What tickers do you want to see values for. Separated by commas")
    parser.add_argument("-s", "--sleep", type=int, default=10,
                    help="How many seconds to sleep for between iterations")
    args = parser.parse_args()

    getStockPrices(args.tickers, args.sleep)
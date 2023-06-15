import os
import zipfile
import requests
from pathlib import Path
from datetime import datetime, timedelta
import time

import sqlite3

conn = sqlite3.connect('stockup.db')
# Open a cursor to perform database operations
cur = conn.cursor()

download_path = os.path.join(str(Path(__file__).resolve().parent.parent.parent), "downloads")
print(download_path)

supported_exchanges = ["bse", "nse"]

Path(download_path).mkdir(parents=True, exist_ok=True)

    # Make sure we also have other directories
for current_exchange in supported_exchanges:
    Path(os.path.join(download_path, current_exchange)).mkdir(parents=True, exist_ok=True)
    Path(os.path.join(download_path, current_exchange,'csv')).mkdir(parents=True, exist_ok=True)


def yesterday():
    """
    formats date in british format
    """
    yesterday = datetime.now() - timedelta(days=1)
    return str(yesterday.date().strftime("%d/%m/%Y"))

def download(download_url, file_path):
    """
    download function is used to fetch the data
    """
    # print("Downloading file at", file_path)

    # Don't download file if we've done that already
    print(file_path)
    if not os.path.exists(file_path):
        file_to_save = open(file_path, "wb")
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'}
  
            with requests.get(download_url, headers=headers, timeout=5, verify=False, stream=True) as response:
                for chunk in response.iter_content(chunk_size=1024):
                    file_to_save.write(chunk)
        except Exception as e:
            print(e)
        print("Completed downloading file")
    else:
        print("We already have this file cached locally")

def download_and_unzip(download_url, file_path,csv_path):
    """
    download_and_unzip takes care of both downloading and uncompressing
    """
    download(download_url, file_path)
    try:
        with zipfile.ZipFile(file_path, "r") as compressed_file:
            # print(csv_path)
            compressed_file.extractall(Path(csv_path))
    except Exception as e:
            print(e)
    # print("Completed un-compressing")

def download_nse_bhavcopy(for_date):
    """
    this function is used to download bhavcopy from NSE
    """
    for_date_parsed = datetime.strptime(for_date, "%d/%m/%Y")
    month = for_date_parsed.strftime("%b").upper()
    year = for_date_parsed.year
    day = "%02d" % for_date_parsed.day
    url = f"https://www.nseindia.com/content/historical/EQUITIES/{year}/{month}/cm{day}{month}{year}bhav.csv.zip"
    file_path = os.path.join(download_path, "nse", f"cm{day}{month}{year}bhav.csv.zip")
    csv_path = os.path.join(download_path, "nse", 'csv')
    try:
        download_and_unzip(url, file_path,csv_path)
    except zipfile.BadZipFile:
        # print(f"Skipping downloading data for {for_date}")
        return
    # commenting so that cache functions works
    # os.remove(file_path)

def download_bse_bhavcopy(for_date):
    """
    this function is used to download bhavcopy from BSE
    """
    for_date_parsed = datetime.strptime(for_date, "%d/%m/%Y")
    month = "%02d" % for_date_parsed.month
    day = "%02d" % for_date_parsed.day
    year = for_date_parsed.strftime("%y")
    file_name = f"EQ{day}{month}{year}_CSV.ZIP"
    # https://www.bseindia.com/download/BhavCopy/Equity/EQ210520_CSV.ZIP
    url = f"http://www.bseindia.com/download/BhavCopy/Equity/{file_name}"
    print(url)
    # time.sleep(12)
    file_path = os.path.join(download_path, "bse", file_name)
    csv_path = os.path.join(download_path, "bse", 'csv')
    try:
        download_and_unzip(url, file_path,csv_path)
    except zipfile.BadZipFile:
        pass
        # print(f"Skipping downloading data for {for_date}")
    # commenting so that cache functions works
    # os.remove(file_path)

def downloader(exchange, for_date, for_past_days):
    """
    download_bhavcopy is utility that will download daily bhav copies
    from NSE and BSE

    Examples:
    python download_bhavcopy.py bse --for_date 06/12/2017

    python download_bhavcopy.py bse --for_past_days 15
    """
   

    # We need to fetch data for past X days
    if for_past_days != 1:
        for i in range(for_past_days):
            ts = datetime.now() - timedelta(days=i+1)
            ts = ts.strftime("%d/%m/%Y")
            if exchange == "nse":
                download_nse_bhavcopy(ts)
            else:
                download_bse_bhavcopy(ts)
    else:
        if exchange == "nse":
            download_nse_bhavcopy(for_date)
        else:
            download_bse_bhavcopy(for_date)

def downloadall_for_past_days(for_past_days):
    # downloader('nse',None,for_past_days=for_past_days)
    downloader('bse',None,for_past_days=for_past_days)
    conn.commit()
    conn.close()
if __name__ == "__main__":
    downloadall_for_past_days(21)
    
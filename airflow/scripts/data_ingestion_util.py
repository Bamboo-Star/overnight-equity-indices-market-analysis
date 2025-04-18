import os
import pandas as pd
import yfinance as yf
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data')))

from google.cloud import storage


# download history trading data from yahoo finance and clean dataframe
def trading_data_download_clean(tickers, start_month, end_month, output_path):
    df_raw = yf.download(eval(tickers), start=start_month+"-01", end=end_month+"-01", auto_adjust=False)
    df_flat = df_raw.reset_index()
    df_flat = df_flat.stack(level=1).reset_index()  # Moves Ticker (level=1) into a column
    df_flat["Date"] = df_flat["Date"].ffill()
    df_flat.dropna(inplace=True)
    df_flat.drop(["level_0"], axis=1, inplace=True)
    df_flat.rename_axis(None, axis=1, inplace=True)
    df_flat.reset_index(drop=True, inplace=True)
    df_flat.to_csv(output_path, index=False)
    return

# default country dict
country_dict = {
    'QQQ': 'United States',
    'XIU.TO': 'Canada',
    'EWW': 'Mexico',
    'EWZ': 'Brazil',
    '000001.SS': 'China',
    '^HSI': 'Hong Kong',
    'ES3.SI': 'Singapore',
    '^N225': 'Japan',
    '^KS11': 'Korea',
    '^BSESN': 'India',
    'STW.AX': 'Australia',
    '^FCHI': 'France',
    '^GDAXI': 'Germany',
    'IMIB.MI': 'Italy',
    '^TA125.TA': 'Israel'
}

# download ticker country data from yahoo finance
def country_data_download_clean(tickers, output_path):
    data = []
    for ticker in eval(tickers):
        print(ticker)
        info = yf.Ticker(ticker).info
        data.append({
            "Ticker": ticker,
            "Name": info.get("shortName"),
            "Country": info.get("country")
        })

    df = pd.DataFrame(data)
    df['Country'] = df['Country'].fillna(df['Ticker'].map(country_dict))
    df.to_csv(output_path, index=False)
    return

# data upload to big query using clustering and partitioning
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    print(f"Using credentials from: {credentials_path}")
    if not os.path.exists(credentials_path):
        print(f"ERROR: Credentials file does not exist at {credentials_path}")

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    return
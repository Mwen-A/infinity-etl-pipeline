import pandas as pd


def extract_csv(file_path: str):
    headers = [
        "datetime",
        "location",
        "name",
        "items",
        "payment-type",
        "price",
        "payment-details",
    ]
    df = pd.read_csv(file_path, names=headers)
    return df


def drop_sensitive_info(df):
    to_drop = ["name", "payment-details"]
    df.drop(columns=to_drop, inplace=True)
    df.drop_duplicates()
    df.dropna()
    return df
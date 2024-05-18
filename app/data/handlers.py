import pymongo.mongo_client
import pymongo.server_api
import requests
import urllib3
import pandas
import dotenv

import datetime
import pathlib
import logging
import timeit
import json
import uuid
import os


dotenv.load_dotenv()
with open("config.json", "r") as f:
    config = json.load(f)
uri = config["mongodb_uri"].format(
    username=os.getenv("MONGODB_USERNAME"),
    password=os.getenv("MONGODB_PASSWORD"),
    cluster_name=os.getenv("MONGODB_CLUSTER_NAME"),
)
client = pymongo.mongo_client.MongoClient(
    uri, server_api=pymongo.server_api.ServerApi("1")
)
database = client.get_database(os.getenv("MONGODB_DATABASE_NAME"))
properties = database.get_collection(os.getenv("MONGODB_COLLECTION_NAME"))
urllib3.disable_warnings()


def insert_data(data_fp: pathlib.Path) -> int:
    data = pandas.read_csv(data_fp)
    total_num_rows = data.shape[0]
    if total_num_rows != 0:
        num_rows_inserted = 0
        batch_size = 1000
        for i in range(0, total_num_rows, batch_size):
            batch = data.iloc[i : i + batch_size]
            records = batch.to_dict(orient="records")
            # Set any values that are NaN to None
            for record in records:
                for k, v in record.items():
                    if pandas.isna(v):
                        record[k] = None
            properties.insert_many(records)
            num_rows_inserted += batch.shape[0]
    return total_num_rows


def parse_data(data_fp: pathlib.Path) -> pathlib.Path:
    data = pandas.read_csv(
        data_fp,
        encoding="ISO-8859-1",
        sep=",",
        skiprows=1,
    )
    data.columns = [
        "sale_date",
        "address",
        "county",
        "eircode",
        "price",
        "is_full_market_price",
        "vat_exclusive",
        "description_of_property",
        "property_size_description",
    ]
    # Generate a unique listing id from the sale date and address
    data["listing_id"] = data.apply(
        lambda x: str(
            uuid.uuid5(uuid.NAMESPACE_DNS, f"{x['sale_date']}_{x['address']}")
        ),
        axis=1,
    )
    # Only keep the rows that are not already in the database
    listing_ids = [row["listing_id"] for row in properties.find({}, {"listing_id": 1})]
    data = data[~data["listing_id"].isin(listing_ids)]
    # Parse the remaining rows
    data["sale_date"] = pandas.to_datetime(
        data["sale_date"], format="%d/%m/%Y"
    ).dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    data["price"] = (
        data["price"]
        .str.replace(" ", "")
        .str.replace(",", "")
        .str.replace("\x80", "")
        .astype(float)
    )
    data["address"] = data["address"].str.title()
    data["vat_exclusive"] = data["vat_exclusive"] == "Yes"
    data["is_full_market_price"] = data["is_full_market_price"] != "Yes"
    data["is_second_hand"] = data["description_of_property"].str.contains("Second-Hand")
    data.drop(columns=["description_of_property"], inplace=True)
    data["eircode"] = data["eircode"].replace({pandas.NA: None})
    data["property_size_description"] = data["property_size_description"].replace(
        {pandas.NA: None}
    )
    input_file_name = data_fp.stem
    output_fp = data_fp.parent.joinpath(f"{input_file_name}_parsed.csv")
    data.to_csv(output_fp, index=False)
    return output_fp


def download_data_from_url(
    download_url: str, download_dir: pathlib.Path
) -> pathlib.Path:
    os.makedirs(download_dir, exist_ok=True)
    data_file_name = download_url.split("/")[-1]
    data_fp = download_dir.joinpath(data_file_name)
    response = requests.get(download_url, verify=False)
    response_content = response.content
    with open(data_fp, "wb") as f:
        f.write(response_content)
    return data_fp


def clean_up_downloads(download_dir: pathlib.Path):
    for fp in download_dir.glob("*"):
        if fp.is_file():
            fp.unlink()
    download_dir.rmdir()


def download_handler(date: datetime.datetime) -> dict:
    total_start_time = timeit.default_timer()
    download_url = config["download_url"].format(
        date="-".join(date.date().isoformat().split("-")[:2])
    )
    download_dir = pathlib.Path(__file__).parent.joinpath(config["download_dir"])
    try:
        data_fp = download_data_from_url(
            download_url=download_url,
            download_dir=download_dir,
        )
    except Exception as e:
        logging.exception(e)
        raise Exception(f"Failed to download data from {download_url}: {e}")
    try:
        parsed_data_fp = parse_data(data_fp)
    except Exception as e:
        logging.exception(e)
        raise Exception(f"Failed to parse data from {data_fp}: {e}")
    try:
        num_rows_inserted = insert_data(parsed_data_fp)
    except Exception as e:
        logging.exception(e)
        raise Exception(f"Failed to insert data from {parsed_data_fp}: {e}")
    clean_up_downloads(download_dir)
    total_end_time = timeit.default_timer()
    response = {
        "download_url": download_url,
        "num_rows_inserted": num_rows_inserted,
        "total_time_seconds": total_end_time - total_start_time,
    }
    return response


if __name__ == "__main__":
    start_range = datetime.datetime(2023, 1, 1)
    end_range = datetime.datetime(2024, 5, 31)
    for date in pandas.date_range(start=start_range, end=end_range, freq="ME"):
        response = download_handler(date)
        print(response)

import motor.motor_asyncio
import pymongo.server_api
import dotenv

import datetime
import json
import os

dotenv.load_dotenv()
with open("config.json", "r") as f:
    config = json.load(f)
uri = config["mongodb_uri"].format(
    username=os.getenv("MONGODB_USERNAME"),
    password=os.getenv("MONGODB_PASSWORD"),
    cluster_name=os.getenv("MONGODB_CLUSTER_NAME"),
)
client = motor.motor_asyncio.AsyncIOMotorClient(
    uri, server_api=pymongo.server_api.ServerApi("1")
)
database = client.get_database(os.getenv("MONGODB_DATABASE_NAME"))
properties = database.get_collection(os.getenv("MONGODB_COLLECTION_NAME"))


async def search_properties_handler(
    address: str = None,
    county: str = None,
    start_date: str = None,
    end_date: str = None,
    min_price: float = None,
    max_price: float = None,
    is_second_hand: bool = None,
    is_descending: bool = True,
    page_num: int = 1,
    limit: int = config["limit_results_per_request"],
) -> dict:
    search_query = {}
    if address:
        address_chunks = address.split(" ")
        address_regex = ".*".join(address_chunks)
        search_query["address"] = {"$regex": address_regex}
    if county:
        search_query["county"] = county.title()
    if start_date:
        start_date_string = datetime.datetime.strptime(
            start_date, "%Y-%m-%d"
        ).isoformat()
        search_query["sale_date"] = {"$gte": start_date_string}
    if end_date:
        end_date_string = (
            datetime.datetime.strptime(end_date, "%Y-%m-%d")
            + datetime.timedelta(days=1)
        ).isoformat()
        if "sale_date" in search_query:
            search_query["sale_date"]["$lte"] = end_date_string
        else:
            search_query["sale_date"] = {"$lte": end_date_string}
    if min_price:
        search_query["price"] = {"$gte": min_price}
    if max_price:
        max_price += 1
        if "price" in search_query:
            search_query["price"]["$lte"] = max_price
        else:
            search_query["price"] = {"$lte": max_price}
    if is_second_hand is not None:
        search_query["is_second_hand"] = is_second_hand
    total_num_documents = await properties.count_documents(search_query)
    cursor = (
        properties.find(search_query, {"_id": 0, "listing_id": 0})
        .skip((page_num - 1) * limit)
        .limit(limit)
        .sort("sale_date", pymongo.DESCENDING if is_descending else pymongo.ASCENDING)
        .batch_size(limit)
    )
    results_list = await cursor.to_list(length=limit)
    has_next_page = total_num_documents > page_num * limit
    response = {
        "total_num_documents": total_num_documents,
        "has_next_page": has_next_page,
        "next_page_num": page_num + 1 if has_next_page else None,
        "num_documents_returned": len(results_list),
        "results": results_list,
    }
    return response

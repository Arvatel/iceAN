from aiohttp import web
from pymongo import MongoClient
import datetime
import os.path

from create_files import create_file


def graph(data):
    """connecting with MongoDB and getting data for graphics"""

    city = data["city"]
    business_type = data["business_type"]

    dt_today = datetime.datetime.today()

    dt_history = int(data["dt_start"])
    dt_start = dt_today + datetime.timedelta(days=-(dt_history * 30))

    dt_prediction = int(data["dt_stop"])
    dt_stop = dt_today + datetime.timedelta(days=(dt_prediction * 30))

    user = "user"
    password = "user"
    host = "95.183.13.86:27017"
    db_name = "City" + city

    # uri = f"mongodb://{user}:{password}@{host}"
    # db = MongoClient(uri)[db_name]

    db = MongoClient("95.183.13.86:27017")[db_name]

    history = db.get_collection("history_new").aggregate(
        pipeline=[
            {
                "$match": {
                    "type": business_type,
                    "ds": {"$gte": dt_start, "$lte": dt_today},
                }
            },
            {"$group": {"_id": "$ds", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ]
    )
    response = {}
    for doc in history:
        response[doc["_id"].strftime("%Y-%m-%d")] = doc["count"]

    future = db.get_collection("future").aggregate(
        pipeline=[
            {
                "$match": {
                    "type": business_type,
                    "ds": {"$gte": dt_today, "$lte": dt_stop},
                }
            },
            {"$sort": {"ds": 1}},
        ]
    )

    for doc in future:
        if doc["y"] >= 0:
            response[doc["ds"].strftime("%Y-%m-%d")] = doc["y"]
        else:
            response[doc["ds"].strftime("%Y-%m-%d")] = 0

    return response


async def get_data(request):
    """send data for graphics: for each day - count of business"""
    try:
        data = await request.json()     # request (city, business type, dt_start, dt_stop)
    except:
        return web.Response(text="Error: wrong graph data")

    response = {}

    i = 0
    for doc in data.values():
        response[i] = {
            "city": doc["city"],
            "business_type": doc["business_type"],
            "dates": graph(doc),
        }
        i = i + 1

    return web.json_response(response) 


def coordinates(data):
    """connectind with MongoDB, taking data for senging coordinates for map (locations)"""

    city = data["city"]
    business_type = data["business_type"]

    user = "user"
    password = "user"
    host = "95.183.13.86:27017"
    db_name = "City" + city

    # uri = f"mongodb://{user}:{password}@{host}"
    # db = MongoClient(uri)[db_name]

    db = MongoClient("95.183.13.86:27017")[db_name]

    points = db.get_collection("today_points").aggregate(
        pipeline=[{"$match": {"_id": business_type}}]
    )

    response = {"lon_array": [], "lat_array": []}

    for doc in points:
        response["lon_array"] = doc["cords"]["lon_array"]
        response["lat_array"] = doc["cords"]["lat_array"]

    return response


async def get_coordinates(request):
    """ send data with coordinates for map (locations)"""

    try:
        data = await request.json()     # request (city, business type)
    except:
        return web.Response(text="Error: wrong coordinates data")

    response = {}
    i = 0
    for doc in data.values():
        response[i] = {
            "city": doc["city"],
            "business_type": doc["business_type"],
            "cords": coordinates(doc),
        }
        i = i + 1

    return web.json_response(response)


async def get_districts(request):
    """ send html page (map with districts)"""
    try:
        data = await request.json()     # request (city, business type)
    except:
        return web.Response(text="Error")

    city = data["city"]
    business_type = data["business_type"]

    directory = "data/{}_{}".format(city, business_type)
    if not os.path.exists(directory): 
        create_file()                   #create html pages

    with open(directory, "r") as file:
        data = file.read()

    response = {"html": data}

    return web.json_response(response)

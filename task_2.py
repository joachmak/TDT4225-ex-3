

from numpy import empty
from utils.constants import COLLECTION_ACTIVITIES, COLLECTION_USERS, COLLECTION_TRACKPOINTS, FEET_TO_METERS
from utils.db_connector import DbConnector
from pymongo import MongoClient
import utils.db as dbpy
import pprint
client = MongoClient('mongodb://localhost:27017/')


def task_2(connection: DbConnector):
    """Find average activity count per user"""
    users = connection.db[COLLECTION_USERS].find({})
    l = list(map(lambda user: [user["_id"], len(user["activity_ids"])], users))
    sum = 0
    for record in l:
        sum += record[1]
    print(f"Average activity count per user: {round(sum/len(l),2)}")


def task_5(connection: DbConnector):
    """Find transportation modes and group by these. Do not include null"""
    activities = connection.db[COLLECTION_ACTIVITIES].find({})
    dict = {}
    for element in activities:
        elements = str(element)
        array1 = elements.split(",")
        array2 = array1[1]
        if array2!=" 'transportation_mode': None":
            transportation_mode = array2.split(":")[1].replace("'","")
            if transportation_mode in dict:
                dict[transportation_mode]+=1
            else:
                dict[transportation_mode] = 1
    pprint.pprint(dict)



def task_11(connection: DbConnector):
    """Find users with transportation modes and their most used transportation mode"""
    print("Users and their most used transportation mode")
    activities = connection.db[COLLECTION_ACTIVITIES].find({})
    dict = {}
    for element in activities:
        elements = str(element)
        array1 = elements.split(",")
        array2 = array1[1]
        if array2!=" 'transportation_mode': None":
            transportation_mode = array2.split(":")[1].replace("'","")
            user_id = array1[4].split(":")[1].replace("}","")
            if user_id in dict:
                dict[user_id].append(transportation_mode)
            else:
                dict[user_id] = [transportation_mode]
    for key in dict:
        dict[key] = max(set(dict[key]), key=dict[key].count)
    pprint.pprint(dict)

    


    







def main():
    conn: DbConnector = DbConnector()
    task_2(conn)
    task_5(conn)
    task_11(conn)
    conn.close_connection()


if __name__ == "__main__":
    main()

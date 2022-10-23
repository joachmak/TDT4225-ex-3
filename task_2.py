from re import X
from typing import Any

from numpy import empty
from utils.constants import COLLECTION_ACTIVITIES, COLLECTION_USERS, COLLECTION_TRACKPOINTS
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

    
def task_3(connection: DbConnector):
    """ Find top 20 users with the highest number of activities """
    users = connection.db[COLLECTION_USERS].find({})
    l = list(map(lambda user: [user["_id"], len(user["activity_ids"])], users))
    l.sort(key=lambda user: user[1], reverse=True)
    i = 1
    print("pos. [user, activity count]:")
    for record in l[:20]:
        print(f"{i}. {record}")
        i += 1

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

def task_8(connection: DbConnector):
    """Find users with most altitude meters gained"""
    old_activity= None
    old_trackpoint= None
    sum_activity = 0
    trackpoints = connection.db[COLLECTION_TRACKPOINTS].find({})
    dict = {}
    for element in trackpoints[:500000]:
        elements = str(element)
        array1 = elements.split(",")
        activity_id = array1[5].split(":")[1].replace("}","").replace("'","")
        trackpoint = array1[3].split(":")[1].replace("'", "")
        if old_activity == None:
            old_activity = activity_id
            old_trackpoint = trackpoint
        elif old_activity == activity_id:
            if float(trackpoint) > float(old_trackpoint):
                sum_activity += (float(trackpoint) - float(old_trackpoint))
            old_activity = activity_id
        elif old_activity != activity_id:
            if sum_activity!=0:
                dict[old_activity] = sum_activity
            old_trackpoint = trackpoint
            sum_activity = 0
            old_activity = activity_id

    pprint.pprint(dict)

    users_with_altitude = {}
    for key in dict:
        user_id = connection.db[COLLECTION_ACTIVITIES].find({"_id": key})[0]["user_id"]
        if user_id in users_with_altitude:
            users_with_altitude[user_id] += dict[key]
        else:
            users_with_altitude[user_id] = dict[key]
    pprint.pprint(users_with_altitude)


        





            
"""            if get_altitude(connection,n)>get_altitude(connection,n-1):
                print("yes")
        n+=1"""


""""        elements = str(element)
        array1 = elements.split(",")
        activity_id = array1[5].split(":")[1].replace("'","").replace("}","")
        altitude = array1[3].split(":")[1].replace("'","")
        if activity_id in dict:
            dict[activity_id].append(altitude)

        else:
            dict[activity_id] = [altitude]
    print(dict)
"""

def task_11(connection: DbConnector):
    """Find users with transportation modes and their most used transportation mode"""
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
    #task_2(conn)
    #task_3(conn)
    #task_5(conn)
    task_8(conn)
    #task_11(conn)
    conn.close_connection()


if __name__ == "__main__":
    main()

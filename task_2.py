

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

def check_float(x):
    try:
        float(x)
        return True
    except ValueError:
        return False


def task_8(connection: DbConnector):
 """create dictionary of activities with their altitude gained"""
 trackpoints = connection.db[COLLECTION_TRACKPOINTS].find({})
 activities = connection.db[COLLECTION_ACTIVITIES].find({})
 dict_activities = {}
 prev_trackpoint = trackpoints[0]
 for trackpoint in trackpoints:
    if trackpoint["alt"]!=-777:
        if trackpoint["activity_id"] == prev_trackpoint["activity_id"]:
            if check_float(trackpoint["alt"]) and check_float(prev_trackpoint["alt"]):
                if float(trackpoint["alt"]) > float(prev_trackpoint["alt"]):
                    if trackpoint["activity_id"] in dict_activities:
                        dict_activities[trackpoint["activity_id"]] += float(trackpoint["alt"]) - float(prev_trackpoint["alt"])
                    else:
                        dict_activities[trackpoint["activity_id"]] = float(trackpoint["alt"]) - float(prev_trackpoint["alt"])
        prev_trackpoint = trackpoint
 pprint.pprint(dict_activities)
 """dict_users = {}   
 for activity in activities:
    print("Finding altitude for activity: " +activity["_id"] + " for user: " + activity["uid"])
    if activity["uid"] in dict_users:
        dict_users[activity["uid"]] += dict_activities[activity["_id"]]
    else:  
        dict_users[activity["uid"]] = dict_activities[activity["_id"]]
 sorted_user_altitude = dict(sorted(dict_users.items(), key=lambda item: item[1], reverse=True))
 first_20_values = dict(list(sorted_user_altitude.items())[:20])
 for key in first_20_values:
    first_20_values[key] *= FEET_TO_METERS
    print("User: " + key + " altitude: " + str(first_20_values[key]))
 pprint.pprint(first_20_values)"""







        


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

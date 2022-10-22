import utils
from classes.db import Db
from classes.user import User
from classes.activity import Activity
from utils.constants import COLLECTION_ACTIVITIES, COLLECTION_USERS
from utils.db_connector import DbConnector


def create_coll(conn: DbConnector, collection_name):
    collection = conn.db.create_collection(collection_name)    
    print('Created collection: ', collection)


def insert_users(conn: DbConnector):
    """ Inserts users based on directory structure """
    all_users = utils.os.get_all_users()
    labeled_users = utils.os.get_labeled_ids()
    users_to_insert = list(map(lambda usr: (usr, usr in labeled_users), all_users))
    #print(users_to_insert)

    user_list = []
    for user in users_to_insert:
        user_obj = User(user[0], user[1], [])
        user_list.append(vars(user_obj))

    #print(user_list)
    utils.db.insert_many(conn, COLLECTION_USERS, user_list)
        

def insert_activity(conn: DbConnector):
    """ Inserts activity for user """
    activites = utils.os.get_activities()

    activity_list = []
    for item in activites:
        activity_obj = Activity(uid = item[3], transportation_mode= item[2], end_time = item[1], start_time = item[0])
        #print(vars(activity_obj))
        activity_list.append(vars(activity_obj))
    print(activity_list)

    utils.db.insert_many(conn, COLLECTION_ACTIVITIES, activity_list)


def get_activity_dict(conn: DbConnector):
    activities = utils.db.get_all(conn, COLLECTION_ACTIVITIES)
    activities_dict = {}
    for activity in activities:
        start_date = activity["start_time"]
        if start_date in activities_dict:
            activities_dict[start_date] = [activities_dict[start_date], activity]
        else:
            activities_dict[start_date] = activity
    return activities_dict


def insert_trackpoints(conn: DbConnector):
    activities_dict: dict = get_activity_dict(conn)
    print(utils.os.get_trackpoints("010", activities_dict))


def main():
    #conn: DbConnector = DbConnector()
    #create_coll(conn, COLLECTION_USERS)
    #create_coll(conn, COLLECTION_ACTIVITIES)
    #create_coll(conn, COLLECTION_TRACKPOINTS)
    #insert_users(conn)
    #insert_activity(conn)
    #insert_trackpoints(conn)
    utils.os.get_activities()





    #conn.close_connection()


if __name__ == "__main__":
    main()

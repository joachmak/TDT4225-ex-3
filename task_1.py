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

def main():
    conn: DbConnector = DbConnector()
    # create_coll(conn, COLLECTION_USERS)
    #insert_users(conn)
    insert_activity(conn)



    conn.close_connection()


if __name__ == "__main__":
    main()

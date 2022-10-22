from typing import Any
from utils.constants import COLLECTION_ACTIVITIES, COLLECTION_USERS
from utils.db_connector import DbConnector
from pymongo import MongoClient
import pprint
client = MongoClient('mongodb://localhost:27017/')


def task_2(connection:DbConnector):
    """Find average amount of activities per user"""
    users = connection.db[COLLECTION_USERS].find({})
    user_count=0
    for user in users:
        user_count+=1
    activities = connection.db[COLLECTION_ACTIVITIES].find({})
    activity_count =0
    for activity in activities:
        activity_count+=1

    average_activities =  round(activity_count/user_count, 2)
    print("Average amount of activities per user is " + str(average_activities))

    
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




def main():
    conn: DbConnector = DbConnector()
    task_2(conn)
    #task_3(conn)
    #task_5(conn)
    conn.close_connection()


if __name__ == "__main__":
    main()

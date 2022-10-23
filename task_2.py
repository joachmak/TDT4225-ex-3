from sqlite3 import Cursor
from utils.constants import COLLECTION_USERS, COLLECTION_ACTIVITIES, COLLECTION_TRACKPOINTS
from utils.db_connector import DbConnector


def task_1(connection: DbConnector):
    """ Find the number of users, activites and trackpoints in the database """
    users: Cursor = connection.db[COLLECTION_USERS].find({})
    activities: Cursor = connection.db[COLLECTION_ACTIVITIES].find({})
    trackpoints: Cursor = connection.db[COLLECTION_TRACKPOINTS].find({})
    print(f"Number of users: {len(list(users))}")
    print(f"Number of activities: {len(list(activities))}")
    #print(f"Number of trackpoints: {len(list(trackpoints))}")

    # This is much faster, but deprecated
    print(f"Number of trackpoints: {trackpoints.count()}")

    """
    # This option is massively faster but we get error messages. The suggested update
    # is to use the count_documents() method instead of count() but this is not available
    
    print(f"Number of users: {users.count()}")
    print(f"Number of activities: {activities.count()}")

    """

    users.close()
    activities.close()
    trackpoints.close()


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


def task_4(connection: DbConnector):
    """ Find all the activites with the value taxi  with a unique uid"""
    activities = connection.db[COLLECTION_ACTIVITIES].find({"transportation_mode": "taxi"})
    
    # gets back the cursor object with unique uids
    unique_uids = activities.distinct("uid")
    
    print(f"Number of activities with with a unique uid: {len(unique_uids)}")
    activities.close()


def task_7(connection: DbConnector):
    # To be continued, removed temporarily
    pass

def task_10(connection: DbConnector):
    """ 
    Finds all the users that have been in the forbidden city by mapping users with trackpoints
    then checking if lat and long corresponds with lat = 39.916 and lon = 116.397. This is done by mapping
    users to an activity on _id and uid and activites to trackpoints on id and activity_id. We get these from the mongodb
    """
    users: Cursor = connection.db[COLLECTION_USERS].find({})
    activities: Cursor = connection.db[COLLECTION_ACTIVITIES].find({})
    trackpoints: Cursor = connection.db[COLLECTION_TRACKPOINTS].find({})


    # Iterate through trackpoints, if lat == 39.916 and lon == 116.397 we map
    # those trackpoints to activities and append them to a list along with uid in activities
    liste_of_users = []
    for t in trackpoints:
        #print(t["lat"], t["lon"])
        if round(t["lat"], 3) == 39.916 and round(t["lon"], 3) == 116.397:
            #print(t)
            # map trackpoints to activities on activity_id and id
            for a in activities:
                if t["activity_id"] == a["_id"]:
                    liste_of_users.append(a["uid"])
                    print(liste_of_users)

    print(liste_of_users)


    users.close()
    activities.close()
    trackpoints.close()

def main():
    conn: DbConnector = DbConnector()
    #task_1(conn)
    #task_3(conn)
    #task_4(conn)

    #task_7(conn)
    task_10(conn)
    conn.close_connection()


if __name__ == "__main__":
    main()

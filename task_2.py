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


def distance(in_lat, in_long, in_lat2, in_long2):
    lat_dist = abs(in_lat2 - in_lat)
    long_dist = abs(in_long2 - in_long)
    #print(f"lat1: {in_lat}, lon1: {in_long} lat2: {in_lat2}, lon2: {in_long2}")
    
    #Assuming that the Earth is a sphere with a circumference of 40075 km.
    #adjust for earth curvature
    #This adjusts for 1 degree of latitude/longitude
    len_lat = 111.32 #km
    len_lon = 40075* np.cos( in_lat ) / 360 #km
    euclidean_dist = np.sqrt((lat_dist*len_lat)**2 + (long_dist*len_lon)**2)


    #print(f"Distance: {euclidean_dist} km")
    return euclidean_dist    

def task_7(connection: DbConnector):
    import pandas as pd
    #print_task_number(7)

    users: Cursor = connection.db[COLLECTION_USERS].find({})
    activities: Cursor = connection.db[COLLECTION_ACTIVITIES].find({})
    trackpoints: Cursor =connection.db[COLLECTION_TRACKPOINTS].aggregate([{
        "$match": {
            "uid": "112"
        }
    }])

    
    
    #users_df = pd.DataFrame(list(users))
    df = pd.DataFrame(list(trackpoints))
    #trackpoints_df = pd.DataFrame(list(trackpoints))

    print("Finding distance walked for user 112...")
    last = None

    #list of different distances, always start with 0 and not starting/ending points between 
    # activities
    distance_sorted = [] 
    list_of_distance = [] #Used for tmp storage of distances
    for e in df.itertuples():
        if last:
            #Create new list if we change activity
            if last_activity_id != e.activity_id:
                distance_sorted.append(list_of_distance)
                list_of_distance = []
                list_of_distance.append(0)
            else:
                list_of_distance.append(distance(e[4], e[5], last[4], last[5]))
        else:
            list_of_distance.append(0)
        last = e
        last_activity_id = e[3]
    
    #Sum distances for different activities
    sum_of_distance = 0
    for item in distance_sorted:
        sum_of_distance += sum(item)
    print(f"Total distance walked for user_id 112 (in km): {sum_of_distance}")
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

    task_7(conn)
    #task_10(conn)
    conn.close_connection()


if __name__ == "__main__":
    main()

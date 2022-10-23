from sqlite3 import Cursor
from utils.constants import COLLECTION_USERS, COLLECTION_ACTIVITIES, COLLECTION_TRACKPOINTS
from utils.db_connector import DbConnector
import pprint


def print_task_message(task_no: int):
    print(f"\n\n============ TASK {task_no} ============\n")

def task_1(connection: DbConnector):
    print_task_message(1)
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


def task_2(connection: DbConnector):
    """Find average activity count per user"""
    print_task_message(2)
    users = connection.db[COLLECTION_USERS].find({})
    user_list = list(map(lambda user: [user["_id"], len(user["activity_ids"])], users))
    activity_count = 0
    for record in user_list:
        activity_count += record[1]
    print(f"Average activity count per user: {round(activity_count/len(user_list),2)}")



def task_3(connection: DbConnector):
    """ Find top 20 users with the highest number of activities """
    print_task_message(3)
    pipeline = [
        {
            "$project": {
                "number of activities": {
                    "$size": "$activity_ids"
                }
            },
        },
        {
            "$sort": {"number of activities": -1}
        },
        {
            "$limit": 20
        }
    ]
    users = connection.db[COLLECTION_USERS].aggregate(pipeline)
    print("Top 20 users (_id) with highest number of activities:")
    for i in range(20):
        print(f"{i + 1}. {users.next()}")


def task_4(connection: DbConnector):
    """ Find all the activites with the value taxi  with a unique uid"""
    print_task_message(4)
    activities = connection.db[COLLECTION_ACTIVITIES].find({"transportation_mode": "taxi"})
    
    # gets back the cursor object with unique uids
    unique_uids = activities.distinct("uid")
    
    print(f"Number of activities with taxi with with a unique uid: {len(unique_uids)}")
    print("These users are:")
    print(unique_uids)
    activities.close()




def task_5(connection: DbConnector):
    """ Find all types of transportation modes and count how many activities that are
        tagged with these transportation mode labels """
    print_task_message(5)
    activities = connection.db[COLLECTION_ACTIVITIES].find({})  # get all activities
    t_mode_dict = {}
    for activity in activities:
        transportation_mode = activity["transportation_mode"]
        if transportation_mode is None:
            continue
        if transportation_mode not in t_mode_dict:
            t_mode_dict[transportation_mode] = 0
        t_mode_dict[transportation_mode] += 1
    pprint.pprint(t_mode_dict)


def task_6(connection: DbConnector):
    """ a) Find the task with the most activities. b) Is this also the year with the most recorded hours? """
    print_task_message(6)
    convert_start_and_end_date_to_date_obj = {
            "$project": {
                "start_time": {
                    "$toDate": "$start_time"
                },
                "end_time": {
                    "$toDate": "$end_time"
                }
            }
        }
    project_start_year_and_date_diff = {
        "$project": {
            "start_year": {
                "$year": "$start_time"
            },
            "duration_in_seconds": {
                "$dateDiff": {
                    "startDate": "$start_time",
                    "endDate": "$end_time",
                    "unit": "second"  # we should use seconds, because if we used hours we would "lose data".
                }
            }
        },
    }
    pipeline = [
        convert_start_and_end_date_to_date_obj,
        project_start_year_and_date_diff,
        {
            "$group": {
                "_id": "$start_year",
                "count": {"$sum": 1}
            },
        },
        {
            "$sort": {
                "count": -1
            },
        },
    ]
    print("Years with most activities:")
    activities_per_year = connection.db[COLLECTION_ACTIVITIES].aggregate(pipeline)
    for year in activities_per_year:
        print(year)
    print("\nYears with most activity seconds:")
    pipeline = [
        convert_start_and_end_date_to_date_obj,
        project_start_year_and_date_diff,
        {
            "$group": {
                "_id": "$start_year",
                "count": {"$sum": "$duration_in_seconds"}
            },
        },
        {
            "$sort": {
                "count": -1
            },
        },
    ]
    activity_duration_per_year = connection.db[COLLECTION_ACTIVITIES].aggregate(pipeline)
    for year in activity_duration_per_year:
        print(year)
    print("\nWe see that the year with the most activities does not have the most recorded hours "
          "(or in this case, seconds).")


def task_7(connection: DbConnector):
    from haversine import haversine, Unit
    import pandas as pd

    print_task_message(7)
    trackpoints_2: Cursor =connection.db[COLLECTION_TRACKPOINTS].aggregate([{
    "$match": {
        "uid": "112"
        }
    }])
    activities: Cursor = connection.db[COLLECTION_ACTIVITIES].aggregate([{
        "$match": {
            "transportation_mode": "walk"
        }
    }])

    df = pd.DataFrame(list(trackpoints_2))
    df_a = pd.DataFrame(list(activities))

    # Merge the two dataframes so we get transortation mode and distance in same dataframe
    print("Merging dataframes...")
    df = pd.merge(df, df_a, left_on="activity_id", right_on="_id")

    activity = ""
    totals = []
    tmp_distances= [0]

    for _, row in df.iterrows():

        # Progress
       #if index%1000 == 0:
            #print(f"Progress: {index}/{len(df)}")

        # Get transportation mode from activity_id in trackpoints from activity collection
        if row["transportation_mode"] != "walk" or row["uid_x"] != "112": 
            continue

        if activity != row["activity_id"]:
            activity = row["activity_id"]
            totals.append(sum(tmp_distances))
            last = (row["lat"], row["lon"])
            tmp_distances = [0]
        else:
            tmp_distances.append(haversine(last, (row["lat"], row["lon"])))
            last = (row["lat"], row["lon"])

    print(f"\nDistance walked for user 112: {sum(totals)} km")


def task_8(connection: DbConnector):
    """ Find the top 20 users who have gained the most altitude meters """
    print_task_message(8)
    user_ids = list(map(lambda usr: usr["_id"], connection.db[COLLECTION_USERS].find({})))
    alt_gained = {}
    user_count = len(user_ids)
    j = 1
    for uid in user_ids:
        alt_gained[uid] = 0
        print(f"Fetching activity data for user {j}/{user_count}, id " + uid)
        j += 1
        # For each user, get array of {_id: activity_id, trackpoints: [list of trackpoints with altitude and time]}
        pipeline = [
            {
                "$match": {
                    "alt": {"$ne": -777},
                    "uid": uid,
                }
            },
            {
                "$group": {
                    "_id": "$activity_id",
                    "trackpoints": {
                        "$push": {
                            "alt": "$alt",
                            "time": "$time",
                        }
                    },
                },
            },
        ]
        activities = connection.db[COLLECTION_TRACKPOINTS].aggregate(pipeline)
        print("\tGot activities! Python code is running now.")
        for activity in activities:
            trackpoints = activity["trackpoints"]
            if len(trackpoints) == 0:  # skip if there are 0 trackpoints for this activity (just in case)
                continue
            altitude_gained = 0
            for i in range(1, len(trackpoints)):
                # iterate through all trackpoints and sum altitude gain
                trackpoint = trackpoints[i]
                prev_trackpoint = trackpoints[i - 1]
                alt_diff = float(trackpoint["alt"]) - float(prev_trackpoint["alt"])
                if alt_diff > 0:  # only add if the diff is positive
                    altitude_gained += alt_diff
            alt_gained[uid] += altitude_gained  # add total activity altitude gain to user's total altitude gain
    # sort users on altitude gained (descending)
    user_altitudes = []
    for user in alt_gained.keys():
        user_altitudes.append([user, alt_gained[user]])
    user_altitudes.sort(key=lambda u_alt: u_alt[1], reverse=True)
    i = 1
    print("\nTop 20 users who have gained the most altitude in meters:")
    print("Pos.\tuid\tmeters ascended")
    for altitude in user_altitudes[:20]:
        print(f"{i}.\t{altitude[0]}\t{round(altitude[1] / 3.2808)}m")  # 1 meter = 3.2808 feet
        i += 1





def task_9(connection: DbConnector):
    """ Find all users who have invalid activities, and the number of invalid activities per user """
    print_task_message(9)
    # 1. Find all invalid activities by going through the trackpoint collection and finding "invalid trackpoints"
    pipeline = [
        {
            "$project": {
                "time": {
                    "$toDate": "$time"
                },
                "previous_time": {
                    "$toDate": "$previous_time"
                },
                "activity_id": "$activity_id",
            },
        },
        {
            "$project": {
                "timediff": {
                    "$dateDiff": {
                        "startDate": "$previous_time",
                        "endDate": "$time",
                        "unit": "second"
                    }
                },
                "activity_id": "$activity_id",
            }
        },
        {
            "$match": {"timediff": {"$gte": 300}}  # greater than or equal to 300 seconds (5 minutes)
        },
        {
            "$group": {
                "_id": "$activity_id",
            },
        },
    ]
    invalid_activities = connection.db[COLLECTION_TRACKPOINTS].aggregate(pipeline)
    # 2. Generate a map of invalid activity_ids for fast lookup
    invalid_activity_map = {}
    for activity in invalid_activities:
        invalid_activity_map[activity["_id"]] = 1
    # 3. For each user, iterate through all activities and count number of invalid activities
    users = connection.db[COLLECTION_USERS].find({})
    user_invalid_activity_count = {}
    for user in users:
        _id = user["_id"]
        activities = user["activity_ids"]
        invalid_activity_count = 0
        for activity in activities:
            if activity in invalid_activity_map:
                invalid_activity_count += 1
        if invalid_activity_count > 0:
            user_invalid_activity_count[_id] = invalid_activity_count
    keys = list(user_invalid_activity_count.keys())
    keys.sort()
    print(f"There are {len(invalid_activity_map.keys())} invalid activities distributed among {len(keys)} users.")
    print("User\tNumber of invalid activities")
    print("-----------------------------------")
    for key in keys:
        print(f"{key}\t{user_invalid_activity_count[key]}")

def task_10(connection: DbConnector):
    """ 
    Finds all the users that have been in the forbidden city by mapping users with trackpoints
    then checking if lat and long corresponds with lat = 39.916 and lon = 116.397. 
    """
    print_task_message(10)
    users: Cursor = connection.db[COLLECTION_USERS].find({})
    activities: Cursor = connection.db[COLLECTION_ACTIVITIES].find({})
    trackpoints: Cursor = connection.db[COLLECTION_TRACKPOINTS].find({})


    # Iterate through trackpoints, if lat == 39.916 and lon == 116.397 we map
    # those trackpoints to activities and append them to a list along with uid in activities
    list_of_users = []
    for t in trackpoints:
        if round(float(t["lat"]), 3) == 39.916 and round(float(t["lon"]), 3) == 116.397 and t["uid"] not in list_of_users: 
            list_of_users.append(t["uid"])
            print(f"Found user {t['uid']} in forbidden city on coordinates lat: {t['lat']} and lon: {t['lon']}")

    print(list_of_users)


    users.close()
    activities.close()
    trackpoints.close()

def task_11(connection: DbConnector):
    """Find users with transportation modes and their most used transportation mode"""
    print_task_message(11)
    print("Users and their most used transportation mode")
    activities = connection.db[COLLECTION_ACTIVITIES].find({})
    user_activity_dict = {}
    for activity in activities:
        transportation_mode = activity["transportation_mode"]
        if transportation_mode is None:
            continue
        uid = activity["uid"]
        if uid not in user_activity_dict:
            user_activity_dict[uid] = []
        user_activity_dict[uid].append(transportation_mode)
    for user in user_activity_dict:
        user_activity_dict[user] = max(set(user_activity_dict[user]), key=user_activity_dict[user].count)
    pprint.pprint(user_activity_dict)


def main():
    conn: DbConnector = DbConnector()
    task_1(conn)
    task_2(conn)
    task_3(conn)
    task_4(conn)
    task_5(conn)
    task_6(conn)
    task_7(conn)
    task_8(conn)
    task_9(conn)
    task_10(conn)
    task_11(conn)
    conn.close_connection()

if __name__ == "__main__":
    main()
import datetime

from utils.constants import COLLECTION_USERS, COLLECTION_ACTIVITIES, COLLECTION_TRACKPOINTS
from utils.db_connector import DbConnector


def print_task_message(task_no: int):
    print(f"\n\n============ TASK {task_no} ============\n")


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





def main():
    conn: DbConnector = DbConnector()
    task_3(conn)
    task_6(conn)
    task_9(conn)
    conn.close_connection()


if __name__ == "__main__":
    main()

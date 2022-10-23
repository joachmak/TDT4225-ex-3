from utils.constants import COLLECTION_USERS, COLLECTION_ACTIVITIES
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
    for i in range(20):
        print(f"{i + 1}. {users.next()}")


def task_6(connection: DbConnector):
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
                    "unit": "second"
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


def main():
    conn: DbConnector = DbConnector()
    task_3(conn)
    task_6(conn)
    conn.close_connection()


if __name__ == "__main__":
    main()

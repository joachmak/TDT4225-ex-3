from utils.constants import COLLECTION_USERS
from utils.db_connector import DbConnector


def task_3(connection: DbConnector):
    """ Find top 20 users with the highest number of activities """
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


def main():
    conn: DbConnector = DbConnector()
    task_3(conn)
    conn.close_connection()


if __name__ == "__main__":
    main()

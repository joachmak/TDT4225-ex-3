from utils.constants import COLLECTION_USERS
from utils.db_connector import DbConnector


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
    task_3(conn)
    conn.close_connection()


if __name__ == "__main__":
    main()

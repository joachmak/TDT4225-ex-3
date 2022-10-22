import logging
import os

import utils
from classes.user import User
from utils.constants import COLLECTION_ACTIVITIES, COLLECTION_USERS, COLLECTION_TRACKPOINTS
from utils.db import drop_collections, create_collections
from utils.db_connector import DbConnector


def insert_users(conn: DbConnector, user_activity_mappings: dict):
    """ Inserts users based on directory structure """
    all_users = utils.os.get_all_users()
    labeled_users = utils.os.get_labeled_ids()
    users_to_insert = list(map(lambda usr: (usr, usr in labeled_users), all_users))

    user_list = []
    for user in users_to_insert:
        if user[0] not in user_activity_mappings:
            logging.getLogger("insert_users").error(f"user {user} not in activity dictionary.")
            continue
        user_obj = User(user[0], user[1], user_activity_mappings[user[0]])
        user_list.append(vars(user_obj))
    print(f"Inserting {len(user_list)} users...")
    utils.db.insert_many(conn, COLLECTION_USERS, user_list)


def configure_logger():
    try:
        os.mkdir("log")
    except FileExistsError:
        pass
    logging.basicConfig(filename="log/task_1.log",
                        filemode="a",
                        format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S",
                        level=logging.DEBUG)


def main():
    configure_logger()
    all_collections: list = [COLLECTION_USERS, COLLECTION_TRACKPOINTS, COLLECTION_ACTIVITIES]
    conn: DbConnector = DbConnector()
    drop_collections(conn, all_collections)
    create_collections(conn, all_collections)
    user_activity_id_mappings = utils.os.upload_activities_and_trackpoints(conn)
    insert_users(conn, user_activity_id_mappings)
    conn.close_connection()


if __name__ == "__main__":
    main()

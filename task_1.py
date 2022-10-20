import utils
from classes.db import Db


def create_coll(self, collection_name):
    collection = self.db.create_collection(collection_name)    
    print('Created collection: ', collection)


def insert_users(db):
    """ Inserts users based on directory structure """
    all_users = utils.os.get_all_users()
    labeled_users = utils.os.get_labeled_ids()
    users_to_insert = list(map(lambda usr: (usr, usr in labeled_users), all_users))
    print(users_to_insert)

    """
    utils.db.insert_rows(db,
                         queries.INSERT_USER,
                         users_to_insert,
                         f"Inserting {len(users_to_insert)} users")
    """



def main():
    db = Db()
    insert_users(db)


    db.connection.close_connection()


if __name__ == "__main__":
    main()

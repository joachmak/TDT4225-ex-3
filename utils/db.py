""" Db operations """
from utils.db_connector import DbConnector


def insert_class_data(connection: DbConnector, collection: str, data: list, msg: str = None) -> None:
    """
    Insert data from a list of class objects (e.g. list of activities, or list of trackpoints)
    Args:
        connection: The DbConnector instance that is connected to the database
        collection: The name of the collection to insert the data to
        data: The list of objects to insert
        msg: An optional message to print before inserting.
    """
    if msg is not None:
        print(msg)
    insert_many(connection, collection, list(map(lambda record: vars(record), data)))


def insert_many(connection: DbConnector, collection: str, data: list):
    if len(data) == 0:
        return
    connection.db[collection].insert_many(data)


def drop_collections(connection: DbConnector, collections: list):
    for col_name in collections:
        print(f"Dropping collection {col_name}...")
        connection.db[col_name].drop()


def create_coll(conn: DbConnector, collection_name: str):
    collection = conn.db.create_collection(collection_name)
    print('Created collection: ', collection)


def create_collections(connection: DbConnector, collections: list):
    for collection in collections:
        create_coll(connection, collection)

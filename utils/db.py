""" Db operations """
#A class for connecting to the mongodb database
#and performing operations on the database

from utils.db_connector import DbConnector


def insert_many(connection: DbConnector, collection: str, data: list):
    connection.db[collection].insert_many(data)


def get_all(connection: DbConnector, collection: str):
    return connection.db[collection].find({})



def execute_query_get_result(db, query: str, message: str = None, print_success=True):
    """ Execute query that requires no payload. Return True if successful, False otherwise """
    if message is not None:
        print(message)
    try:
        db.cursor.execute(query)
        if print_success:
            print("\t...SUCCESS")
        return db.cursor.fetchall()
    except Exception as e:
        print(e)
        return None
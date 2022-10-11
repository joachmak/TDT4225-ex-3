from utils.db_connector import DbConnector


class Db:
    def __init__(self) -> None:
        """ Sets up connection """
        self.connection = DbConnector()

#
# simpledb - A set of utilities for fetching data from a database.
#

import mysql.connector

class SimpleDb:
    def __init__(self):
        self.db_db = None
        self.db_user = None
        self.db_pw = None
        self.db_host = None
        self.db_port = None
        self.db_type = None
        self.db_conn = None
        self.db_set = False
        self.clear()

    def clear(self):
        return

    def db_init(self, database, user, password, host="127.0.0.1", port=3306, dbtype="mysql"):
        self.db_db = database
        self.db_user = user
        self.db_pw = password
        self.db_host = host
        self.db_port = port
        self.db_type = dbtype
        self.db_set = True

    def db_connect(self, autocommit=True):
        if not self.db_set:
            raise ConnectionError("Connect without DB parameters set")
        if self.db_type == "mysql":
            self.db_conn = mysql.connector.connect(host=self.db_host,
                                                   user=self.db_user,
                                                   passwd=self.db_pw,
                                                   database=self.db_db,
                                                   port=self.db_port,
                                                   autocommit=autocommit)

    def dictlist_query(self, query: str) -> list:
        """
        :query SQL Query String:
        :param query:
        :return:
        """
        retlist = []
        db_cur = self.db_conn.cursor(dictionary=True)
        try:
            db_cur.execute(query)
            db_result = db_cur.fetchall()
        except mysql.connector.ProgrammingError as pe:
            print("ERROR: query [{}]".format(query))
            raise pe
        for row in db_result:
            retlist.append(row)
        return retlist
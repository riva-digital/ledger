"""
 db_connect
 Riva's database connection management module.
 Object manages connecting, committing and closing to the database
 As of now, this is intended to be a prototype. We will be graduating
 to Django for the real implementation
"""

import base64
import MySQLdb


class RivaDatabase(object):
    def __init__(self, hostname="", port=3306, dbname="", dbuser="", passwd=""):
        """
        Initialize the RivaDatabase object
        :param hostname: The IP Address
        :param port: The port to access the database
        :param dbname: The name of the database
        :param dbuser: The master user of the database
        :param passwd: The password of the database. Should be base64 encoded.
        :return:
        """
        self.__db_host = ""
        self.set_db_host(hostname)

        self.__db_port = 3306
        self.set_db_port(port)

        self.__db_name = ""
        self.set_db_name(dbname)

        self.__db_user = ""
        self.set_db_user(dbuser)

        self.__db_pass = ""
        self.set_db_password(passwd)

        self.db = ""
        self.db_cursor = ""

    def set_db_host(self, hostname=""):
        """
        Sets the database host to the one provided.
        :param hostname: The ip address or the host name of the db server
        :return: None
        """
        if hostname == "":
            self.__db_host = "127.0.0.1"
        else:
            self.__db_host = hostname

    def get_db_host(self):
        """
        :return: string The database server name/ip
        """
        return self.__db_host

    def set_db_port(self, port=3306):
        """
        Sets the port on which to access the database on the server. 3306 is the default
        :param port: The port number
        :return: None
        """
        self.__db_port = port

    def get_db_port(self):
        """
        :return: string The port to access the database on
        """
        return self.__db_port

    def set_db_name(self, dbname=""):
        """
        Sets the master db name
        :param dbname: The name of the database
        :return: None
        """
        if dbname == "":
            self.__db_name = "rivadb"
        else:
            self.__db_name = dbname

    def get_db_name(self):
        """
        :return: string The name of the database
        """
        return self.__db_name

    def set_db_user(self, dbuser=""):
        """
        Sets the master user name
        :param dbuser: The master username
        :return: None
        """
        if dbuser == "":
            self.__db_user = "admin"
        else:
            self.__db_user = dbuser

    def get_db_user(self):
        """
        :return: string The master user
        """
        return self.__db_user

    def set_db_password(self, passwd=""):
        """
        Sets the password for the database
        :param passwd: The database password
        :return: None
        """
        if passwd == "":
            passwd = "cml2YTAx"
        else:
            passwd = passwd
        self.__db_pass = passwd

    def get_db_password(self):
        """
        :return: The database password
        """
        return base64.b64decode(self.__db_pass)

    def connect(self):
        self.db = MySQLdb.connect(host=self.get_db_host(),
                                  port=self.get_db_port(),
                                  db=self.get_db_name(),
                                  user=self.get_db_user(),
                                  passwd=self.get_db_password())

        self.db_cursor = self.db.cursor(MySQLdb.cursors.DictCursor)


if "__name__" == "__main__":
    pass

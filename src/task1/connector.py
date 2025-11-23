"""File for MariaDbConnector class"""
import mysql.connector


class MariaDbConnector:
    @staticmethod
    def make_connection():
        cnx = mysql.connector(
            host="localhost",
            port=3306,
            user="mariadb-user",
            password="dev-instance"
        )
        
        return cnx.cursor()
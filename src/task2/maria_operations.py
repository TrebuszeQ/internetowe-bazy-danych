"""File for MariaDB operations"""
from logging import Logger, getLogger
from typing import Optional

import mysql.connector
from mysql.connector import errorcode

log: Logger = getLogger(__name__)

_HOST: str = "localhost"
_PORT: int = 3306
_USER: str = "root"
_DB_PASSWD: str = "dev-instance"
_DB: str = "python_db"

def _get_connection():
    try:
        return mysql.connector.connect(
            host=_HOST,
            port=_PORT,
            user=_USER,
            password=_DB_PASSWD
        )

    except Exception as e:
        log.warning("Failed to connect to %s:%s", _HOST, _PORT)
        return None

def _get_pydb_connection():
    try:
        return mysql.connector.connect(
            host=_HOST,
            db=_DB,
            port=_PORT,
            user=_USER,
            password=_DB_PASSWD
        )

    except Exception as e:
        log.warning("Failed to connect to %s:%s", _HOST, _PORT)
        return None

def db_init() -> None:
    """Creates python_db"""
    cnx = _get_connection()

    if cnx is not None:
        cursor = cnx.cursor()
        try:
            cursor.execute(f"CREATE DATABASE {_DB};")
            log.info("%s created", _DB)

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_DB_CREATE_EXISTS:
                pass

        except Exception as e:
            log.warning("Failed to create %S", _DB)

        cursor.close()
        cnx.close()


def mysqli_logs_init():
    cnx = _get_pydb_connection()
    if cnx is not None:
        cursor = cnx.cursor()

        query_dict: dict[str, str] = {
            "create_table_query": """CREATE TABLE mysqli_logs (
                        log_id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id INT,
                        action_type ENUM('INSERT', 'UPDATE', 'DELETE'),
                        action_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );""",
            "create_triggers_query": """CREATE TRIGGER [IF NOT EXISTS] after_user_insert
                    AFTER INSERT ON mysqli_users
                    FOR EACH ROW
                    BEGIN
                        INSERT INTO mysqli_logs (user_id, action_type)
                        VALUES (NEW.id, 'INSERT');
                    END;

                    CREATE TRIGGER after_user_update [IF NOT EXISTS]
                    AFTER UPDATE ON mysqli_users
                    FOR EACH ROW
                    BEGIN
                        INSERT INTO mysqli_logs (user_id, action_type)
                        VALUES (NEW.id, 'UPDATE');
                    END;

                    CREATE TRIGGER after_user_delete [IF NOT EXISTS]
                    AFTER DELETE ON mysqli_users
                    FOR EACH ROW
                    BEGIN
                        INSERT INTO mysqli_logs (user_id, action_type)
                        VALUES (OLD.id, 'DELETE');
                    END;"""
        }

        for key, query in query_dict.items():
            log.info("Executing query: %s", key)
            try:
                cursor.execute(query)
                log.info("Query %s executed", key)

            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    pass

            except Exception as e:
                log.warning("Failed to get users: %s", e)

        cursor.close()
        cnx.close()

def get_all_users() -> Optional[list]:
    cnx = _get_pydb_connection()
    users_list: list = []

    if cnx is not None:
        cursor = cnx.cursor()

        try:
            cursor.execute("SELECT User, Host FROM mysql.user;")
            results = cursor.fetchall()

            for user, host in results:
                users_list.append({
                    "username": user,
                    "host": host
                })
            cursor.close()
            cnx.close()


        except Exception as e:
            log.warning("Failed to get users: %s", e)

        finally:
            if cursor is not None:
                cursor.close()

            if cnx is not None:
                cnx.close()

    return users_list

def add_user(username: str, password: str):
    cnx = _get_pydb_connection()

    if cnx is not None:
        cursor = cnx.cursor()

        try:
            cursor.execute(f"INSERT INTO mysql.user (User, authentication_string, plugin) VALUES ({username}, PASSWORD({password}), 'mysql_native_password');")
            cnx.commit()

            log.info("User %s added", username)
            cursor.close()
            cnx.close()
            return f'{"message": "User added successfully.", "username": "{username}"}'

        except Exception as e:
            log.warning("Failed to add user %s; %s", username, e)
            if cnx:
                cnx.rollback()
            return f'{"message": "Failed to add user.", "username": "{username}"}'

        finally:
            if cursor is not None:
                cursor.close()

            if cnx is not None:
                cnx.close()

    return users_list

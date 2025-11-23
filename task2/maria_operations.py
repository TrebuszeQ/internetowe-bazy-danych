"""File for MariaDB operations"""
from logging import Logger, getLogger
from typing import Optional, Any, Dict, Tuple, List

import mysql.connector
from mysql.connector import errorcode

log: Logger = getLogger(__name__)

_HOST: str = "localhost"
_PORT: int = 3306
_USER: str = "root"
_DB_PASSWD: str = "dev-instance"
_DB: str = "python_db"

ConnResult = Optional[Tuple[mysql.connector.connection.MySQLConnection, Dict[str, Any]]]


def _get_connection() -> ConnResult:
    """
    Establishes a connection to the MariaDB server without specifying a database.
    Returns (connection_object, response_dict) or None if connection fails.
    """
    response: Dict[str, Any] = {}
    cnx = None
    try:
        cnx = mysql.connector.connect(
            host=_HOST,
            port=_PORT,
            user=_USER,
            password=_DB_PASSWD
        )
        response["success"] = True
        response["message"] = f"Connection to {_HOST}:{_PORT} successful"
        return cnx, response

    except Exception as e:
        msg: str = f"Failed to connect to {_HOST}:{_PORT}; {e}"
        log.warning(msg)
        response["success"] = False
        response["message"] = msg
        return None


def _get_pydb_connection() -> ConnResult:
    """
    Establishes a connection to the specific database (_DB).
    Returns (connection_object, response_dict) or None if connection fails.
    """
    response: Dict[str, Any] = {}
    cnx = None
    try:
        cnx = mysql.connector.connect(
            host=_HOST,
            db=_DB,
            port=_PORT,
            user=_USER,
            password=_DB_PASSWD
        )
        response["success"] = True
        response["message"] = f"Connection to {_HOST}:{_PORT}, db {_DB} successful"
        return cnx, response

    except Exception as e:
        msg: str = f"Failed to connect to {_HOST}:{_PORT}, database {_DB} not found or failed: {e}"
        log.warning(msg)
        response["success"] = False
        response["message"] = msg
        return None


def db_init() -> Dict[str, Any]:
    """
    Creates the specified database (_DB) if it doesn't already exist.
    """
    result = _get_connection()

    if result is None:
        return {"success": False, "message": "Failed to connect to server for initialization."}

    cnx, response = result
    cursor = None

    try:
        cursor = cnx.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{_DB}`;")
        log.info(f"Database `{_DB}` ensured to exist.")

        response["success"] = True
        response["message"] = f"Database `{_DB}` created or already exists."
        return response

    except mysql.connector.Error as err:
        response["success"] = False
        response["message"] = f"Error during database creation: {err.msg}"
        log.error(response["message"])
        return response

    except Exception as e:
        msg: str = f"Failed to execute database creation query: {e}"
        log.warning(msg)
        response["success"] = False
        response["message"] = msg
        return response

    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()


def mysqli_logs_init() -> Dict[str, Any]:
    """
    Creates the mysqli_logs table and necessary triggers.
    """
    result = _get_pydb_connection()

    if result is None:
        return {"success": False, "message": "Failed to connect to database for logs initialization."}

    cnx, response = result
    cursor = None

    query_dict: Dict[str, str] = {
        "create_table_query": """
            CREATE TABLE mysql.mysqli_logs (
                log_id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                action_type ENUM('INSERT', 'UPDATE', 'DELETE'),
                action_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );""",
        "create_triggers_query": """
            DROP TRIGGER IF EXISTS after_user_insert;
            CREATE TRIGGER after_user_insert
            AFTER INSERT ON mysqli_users
            FOR EACH ROW
            BEGIN
                INSERT INTO mysqli_logs (user_id, action_type)
                VALUES (NEW.id, 'INSERT');
            END;
            
            DROP TRIGGER IF EXISTS after_user_update;
            CREATE TRIGGER after_user_update
            AFTER UPDATE ON mysqli_users
            FOR EACH ROW
            BEGIN
                INSERT INTO mysqli_logs (user_id, action_type)
                VALUES (NEW.id, 'UPDATE');
            END;

            DROP TRIGGER IF EXISTS after_user_delete;
            CREATE TRIGGER after_user_delete
            AFTER DELETE ON mysqli_users
            FOR EACH ROW
            BEGIN
                INSERT INTO mysqli_logs (user_id, action_type)
                VALUES (OLD.id, 'DELETE');
            END;
        """
    }

    try:
        cursor = cnx.cursor()

        log.info("Executing query: create_table_query")
        try:
            cursor.execute(query_dict["create_table_query"])
            log.info("Table query executed.")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                log.info("Table mysqli_logs already exists. Skipping.")
            else:
                raise

        log.info("Executing query: create_triggers_query")
        trigger_queries = query_dict["create_triggers_query"].split(';')
        for query in trigger_queries:
            q = query.strip()
            if q:
                cursor.execute(q)

        log.info("Triggers created/updated.")
        response["success"] = True
        response["message"] = "Table and triggers initialized successfully."
        return response

    except Exception as e:
        msg: str = f"Failed to initialize logs/triggers: {e}"
        response["success"] = False
        response["message"] = msg
        log.warning(msg)
        return response

    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()


def get_all_users() -> Dict[str, Any]:
    """
    Fetches all usernames and hosts from the mysql.user table.
    Returns a dictionary containing the results or an error message.
    """
    result = _get_pydb_connection()
    users_list: List[Dict[str, str]] = []

    if result is None:
        return {"success": False, "message": "Database connection failed."}

    cnx, response = result
    cursor = None

    try:
        cursor = cnx.cursor(dictionary=True)

        cursor.execute("SELECT User AS username, Host AS host FROM mysql.user;")
        results = cursor.fetchall()

        users_list = results

        response["success"] = True
        response["data"] = users_list
        response["message"] = "Users fetched successfully."

    except Exception as e:
        msg: str = f"Failed to get users: {e}"
        log.warning(msg)
        response["success"] = False
        response["message"] = msg

    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()

    return response


def add_user(username: str, password: str) -> Dict[str, Any]:
    """
    Adds a new user to the mysql.user table using parameterized query.
    """
    result = _get_pydb_connection()

    if result is None:
        return {"success": False, "message": "Database connection failed."}

    cnx, response = result

    if not username or not password:
        response["success"] = False
        response["message"] = "Missing required fields: username and/or password."
        return response

    cursor = None
    try:
        cursor = cnx.cursor()

        query = """
            INSERT INTO mysql.user (User, Host, authentication_string, plugin) 
            VALUES (%s, 'localhost', PASSWORD(%s), 'mysql_native_password')
        """
        cursor.execute(query, (username, password))

        cnx.commit()

        log.info("User %s added", username)
        response["success"] = True
        response["message"] = f"User {username} added successfully"

    except Exception as e:
        msg: str = f"Failed to add user {username}: {e}"
        response["success"] = False
        response["message"] = msg
        log.warning(msg)

        if cnx:
            cnx.rollback()

    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()

    return response


def delete_user(username: str, host: str = 'localhost') -> Dict[str, Any]:
    """
    Deletes a user account from the database using the DROP USER command.
    Host defaults to 'localhost' but can be specified.

    Args:
        username (str): The username to delete.
        host (str): The host associated with the user (e.g., 'localhost', '%').
    Returns:
        Dict[str, Any]: A structured response dictionary.
    """
    result = _get_pydb_connection()

    if result is None:
        return {"success": False, "message": "Database connection failed."}

    cnx, response = result

    if not username:
        response["success"] = False
        response["message"] = "Missing required field: username."
        return response

    cursor = None
    try:
        cursor = cnx.cursor()

        query = "DROP USER IF EXISTS %s@%s;"

        cursor.execute(query, (username, host))

        cnx.commit()

        log.info("Attempted to delete user %s@%s", username, host)
        response["success"] = True
        response["message"] = f"Attempted to delete user {username}@{host}."

    except Exception as e:
        msg: str = f"Failed to delete user {username}@{host}: {e}"
        response["success"] = False
        response["message"] = msg
        log.warning(msg)

        if cnx:
            cnx.rollback()

    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()

    return response


def delete_user_by_id(user_id: int) -> Dict[str, Any]:
    """
    Deletes a user account from the database using the DROP USER command.
    Host defaults to 'localhost' but can be specified.

    Args:
        user_id (id): User id in the table.
    Returns:
        Dict[str, Any]: A structured response dictionary.
    """
    result = _get_pydb_connection()
    host = "localhost"

    if result is None:
        return {"success": False, "message": "Database connection failed."}

    cnx, response = result

    if not user_id:
        response["success"] = False
        response["message"] = "Missing required field: username."
        return response

    cursor = None
    try:
        cursor = cnx.cursor()

        query = "DROP USER IF EXISTS %s@%s;"

        cursor.execute(query, (user_id, host))

        cnx.commit()

        log.info("Attempted to delete user %s@%s", user_id, host)
        response["success"] = True
        response["message"] = f"Attempted to delete user {user_id}@{host}."

    except Exception as e:
        msg: str = f"Failed to delete user {user_id}@{host}: {e}"
        response["success"] = False
        response["message"] = msg
        log.warning(msg)

        if cnx:
            cnx.rollback()

    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()

    return response

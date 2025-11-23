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

def get_all_users() -> Dict[str, Any]:
    """
    Fetches all usernames and hosts from the user table.
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

        cursor.execute("SELECT Username FROM mysqli_users;")
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
    Adds a new user to the mysqli_users table using parameterized query.
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
            INSERT INTO mysqli_users (User, Password) 
            VALUES (%s, PASSWORD(%s))
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


def delete_user_by_id(user_id: int) -> Dict[str, Any]:
    """
    Deletes a user account from the database using the DROP USER command.

    Args:
        user_id (id): User id in the table.
    Returns:
        Dict[str, Any]: A structured response dictionary.
    """
    result = _get_pydb_connection()

    if result is None:
        return {"success": False, "message": "Database connection failed."}

    cnx, response = result

    if not user_id:
        response["success"] = False
        response["message"] = "Missing required field: user_id."
        return response

    cursor = None
    try:
        cursor = cnx.cursor()

        query = "DELETE FROM mysqli_users where ID=%s;"

        cursor.execute(query, user_id)

        cnx.commit()

        log.info("Attempted to delete user %s", user_id)
        response["success"] = True
        response["message"] = f"Attempted to delete user {user_id}."

    except Exception as e:
        msg: str = f"Failed to delete user {user_id}: {e}"
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

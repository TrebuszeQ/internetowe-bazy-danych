"""File for main method"""
import mysql.connector


def main():
    cnx = mysql.connector(
            host="localhost",
            port=3306,
            user="root",
            password="dev-instance"
        )
        
    cursor: cnx.cursor()

    query_dict: dict[str, str] = {
        "create_db_query": "CREATE DATABASE python_db;",
        "create_table_query": """CREATE TABLE mysqli_logs (
            log_id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT,
            action_type ENUM('INSERT', 'UPDATE', 'DELETE'),
            action_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""",
        "create_triggers_query": """CREATE TRIGGER after_user_insert
        AFTER INSERT ON mysqli_users
        FOR EACH ROW
        BEGIN
            INSERT INTO mysqli_logs (user_id, action_type)
            VALUES (NEW.id, 'INSERT');
        END;

        CREATE TRIGGER after_user_update
        AFTER UPDATE ON mysqli_users
        FOR EACH ROW
        BEGIN
            INSERT INTO mysqli_logs (user_id, action_type)
            VALUES (NEW.id, 'UPDATE');
        END;

        CREATE TRIGGER after_user_delete
        AFTER DELETE ON mysqli_users
        FOR EACH ROW
        BEGIN
            INSERT INTO mysqli_logs (user_id, action_type)
            VALUES (OLD.id, 'DELETE');
        END;"""
    }

    for key, query in query_dict.items():
        print("Executing query: ", key)
        cursor.execute(query)
    
    cnx.close()
    


if __name__ == "__main__":
    main()
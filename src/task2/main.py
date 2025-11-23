"""File for main method"""
from logging import Logger, getLogger

from flask import Flask
from flask import jsonify
from maria_operations import db_init, mysqli_logs_init, get_all_users


log: Logger = getLogger("DbApi")
app = Flask("task2")
@app.route("/users", methods=["GET"])
def get_users():
    data = get_all_users()
    return jsonify(data)

@app.route("/users/username/passwod", methods=["POST"])
def add_user(username: str, password: str):
    pass

@app.route("/users/<id>")
def remove_user(user_id: int):
    pass


def main():
    db_init()
    mysqli_logs_init()


if __name__ == "__main__":
    main()
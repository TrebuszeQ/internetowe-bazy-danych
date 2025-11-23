"""File for FlaskApp"""
from logging import Logger, getLogger


from flask import Flask
from flask import jsonify
from task2.maria_operations import get_all_users, delete_user_by_id

log: Logger = getLogger(__name__)
app = Flask(__name__)


@app.route("/users", methods=["GET"])
def get_users():
    data = get_all_users()
    return jsonify(data)

@app.route("/users/username/password", methods=["POST"])
def add_user(username: str, password: str):
    data = add_user(username, password)
    return jsonify(data)

@app.route("/users/<id>")
def remove_user(user_id: int):
    data = delete_user_by_id(user_id)
    return jsonify(data)
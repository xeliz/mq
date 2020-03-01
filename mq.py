#!/usr/bin/env python3

# A simple (primitive) message queue handler in Python
# No authentication supported.
#
# Methods:
#   POST /mq/queue_name/push [ JSON ]        - add a new message to a queue and get its id
#   POST /mq/queue_name/pop [ ?n=how_many ]  - remove and get at most n messages from a queue, default is 1
#   GET /mq/queue_name [ ?n=how_many ]       - get at most n messages from a queue, default is 1
#   GET /mq/queue_name/count                 - get a number of messages in a queue
#   PUT /mq/queue_name                       - create a queue if it does not exist
#   DELETE /mq/queue_name                    - delete a queue if it exists
#   GET /mq                                  - get a list of queues
# If there is an error, a { "error": "description" } is returned.

import flask
import werkzeug.exceptions
import sqlite3
import json

app = flask.Flask(__name__)

# maximum number of messages user may request for
MAX_MESSAGES_REQUEST_NUMBER = 100

# DAO for working with queues
# Has "in-memory" implementation
class QueuesDAO:
    DB_FILE  = "mq.db"

    CHECK_TABLE_EXISTS = "select * from sqlite_master where type='table' and name = ?"

    # TODO: mixing DDL and code is bad, should be refactored.
    CREATE_QUEUES = ("create table queues ("
            + "id integer not null primary key,"
            + "name varchar(100) not null unique)")

    CREATE_MESSAGES = ("create table messages ("
            + "id integer not null primary key,"
            + "queue_id integer not null,"
            + "message text not null,"
            + "foreign key (queue_id) references queues(id))")

    CHECK_QUEUE_EXISTS = "select * from queues where name = ?"

    PUSH_MESSAGE = ("insert into messages (queue_id, message) values ("
            + "(select id from queues where name = ?),"
            + "?)")

    GET_MESSAGES = ("select id, message from messages where queue_id = ("
            + "select id from queues where name = ?)"
            + "order by id asc limit ?")

    DELETE_MESSAGES = "delete from messages where id in "

    COUNT_MESSAGES = ("select count(*) from messages where queue_id = "
            + "(select id from queues where name = ?)")

    CREATE_QUEUE = "insert into queues (name) values (?)"

    DELETE_QUEUE_MESSAGES = ("delete from messages where queue_id = "
            + "(select id from queues where name = ?)")

    DELETE_QUEUE = "delete from queues where name = ?"
    
    LIST_QUEUES = "select name from queues order by name asc"

    def init_db(self):
        con = sqlite3.connect(QueuesDAO.DB_FILE)
        cur = con.cursor()
        if not cur.execute(QueuesDAO.CHECK_TABLE_EXISTS, ("queues",)).fetchall():
            cur.execute(QueuesDAO.CREATE_QUEUES)
            cur.execute("insert into queues (name) values ('default')")
        if not cur.execute(QueuesDAO.CHECK_TABLE_EXISTS, ("messages",)).fetchall():
            cur.execute(QueuesDAO.CREATE_MESSAGES)
        cur.close()
        con.commit()
        con.close()

    # check if a queue (not) exists
    def check_queue(self, qname):
        con = sqlite3.connect(QueuesDAO.DB_FILE)
        cur = con.cursor()
        if not cur.execute(QueuesDAO.CHECK_QUEUE_EXISTS, (qname,)).fetchall():
            raise ValueError("No such queue: '{}'".format(qname))
        cur.close()
        con.commit()
        con.close()

    # add a message at the beginning
    def push(self, qname, message):
        self.check_queue(qname)
        con = sqlite3.connect(QueuesDAO.DB_FILE)
        cur = con.cursor()
        cur.execute(QueuesDAO.PUSH_MESSAGE, (qname, json.dumps(message)))
        message_id = cur.lastrowid
        cur.close()
        con.commit()
        con.close()
        return message_id

    # pop at most n messages from the end
    def pop(self, qname, n=1):
        self.check_queue(qname)
        con = sqlite3.connect(QueuesDAO.DB_FILE)
        cur = con.cursor()
        result = cur.execute(QueuesDAO.GET_MESSAGES, (qname, n))
        ids = []
        messages = []
        for row in result:
            ids.append(row[0])
            messages.append({
                "id": row[0],
                "message": json.loads(row[1]),
            })
        if ids:
            query = QueuesDAO.DELETE_MESSAGES + "(" + ",".join(["?"] * len(ids)) + ")"
            cur.execute(query, ids)
        cur.close()
        con.commit()
        con.close()
        return messages

    # get at most n messages from the end.
    # like pop(), but without removing
    def get(self, qname, n=1):
        self.check_queue(qname)
        con = sqlite3.connect(QueuesDAO.DB_FILE)
        cur = con.cursor()
        result = cur.execute(QueuesDAO.GET_MESSAGES, (qname, n))
        messages = []
        for row in result:
            messages.append({
                "id": row[0],
                "message": json.loads(row[1]),
            })
        cur.close()
        con.commit()
        con.close()
        return messages

    # get number of messages in queue
    def count(self, qname):
        self.check_queue(qname)
        con = sqlite3.connect(QueuesDAO.DB_FILE)
        cur = con.cursor()
        n = cur.execute(QueuesDAO.COUNT_MESSAGES, (qname,)).fetchone()[0]
        cur.close()
        con.commit()
        con.close()
        return n

    # create a queue if it not exists
    def create(self, qname):
        con = sqlite3.connect(QueuesDAO.DB_FILE)
        cur = con.cursor()
        try:
            cur.execute(QueuesDAO.CREATE_QUEUE, (qname,))
        except sqlite3.IntegrityError:
            pass
        cur.close()
        con.commit()
        con.close()

    # delete a queue if it exists
    def delete(self, qname):
        con = sqlite3.connect(QueuesDAO.DB_FILE)
        cur = con.cursor()
        cur.execute(QueuesDAO.DELETE_QUEUE_MESSAGES, (qname,))
        cur.execute(QueuesDAO.DELETE_QUEUE, (qname,))
        cur.close()
        con.commit()
        con.close()

    # returns a list of existing queues
    def list(self):
        con = sqlite3.connect(QueuesDAO.DB_FILE)
        cur = con.cursor()
        queues = cur.execute(QueuesDAO.LIST_QUEUES).fetchall()
        cur.close()
        con.commit()
        con.close()
        return [q[0] for q in queues]

# push message to queue
@app.route("/mq/<qname>/push", methods=["POST"])
def mq_push(qname):
    queuesDAO = QueuesDAO()
    data = flask.request.json
    if data is None:
        raise TypeError("Not a JSON value")
    message_id = queuesDAO.push(qname, data)
    return flask.jsonify({"id": message_id})

# pop messages from queue
@app.route("/mq/<qname>/pop", methods=["POST"])
def mq_pop(qname):
    queuesDAO = QueuesDAO()
    n = flask.request.args.get("n", "1")
    if not n.isdigit():
        raise TypeError("n must be an integer number")
    n = int(n)
    if n < 0:
        raise ValueError("n must be positive")
    if n > MAX_MESSAGES_REQUEST_NUMBER:
        raise ValueError("n must be at most {}".format(MAX_MESSAGES_REQUEST_NUMBER))

    msgs = queuesDAO.pop(qname, n)
    return flask.jsonify(msgs)

# get messages from queue
@app.route("/mq/<qname>")
def mq_get(qname):
    queuesDAO = QueuesDAO()
    n = flask.request.args.get("n", "1")
    if not n.isdigit():
        raise TypeError("n must be an integer number")
    n = int(n)
    if n < 0:
        raise TypeError("n must be positive")
    if n > MAX_MESSAGES_REQUEST_NUMBER:
        raise ValueError("n must be at most {}".format(MAX_MESSAGES_REQUEST_NUMBER))

    msgs = queuesDAO.get(qname, n)
    return flask.jsonify(msgs)

# get numbero f messages from queue
@app.route("/mq/<qname>/count")
def mq_count(qname):
    queuesDAO = QueuesDAO()
    count = queuesDAO.count(qname)
    return flask.jsonify({"count": count})

# create a queue if not exists
@app.route("/mq/<qname>", methods=["PUT"])
def mq_create(qname):
    queuesDAO = QueuesDAO()
    queuesDAO.create(qname)
    return flask.jsonify({})

# delete a queue if exists
@app.route("/mq/<qname>", methods=["DELETE"])
def mq_delete(qname):
    queuesDAO = QueuesDAO()
    queuesDAO.delete(qname)
    return flask.jsonify({})

# get a list of existing queues
@app.route("/mq")
def mq_list():
    queuesDAO = QueuesDAO()
    queues = queuesDAO.list()
    return flask.jsonify(queues)

# handle all the exceptions
@app.errorhandler(Exception)
def handle_server_error(e):
    if isinstance(e, werkzeug.exceptions.HTTPException):
        return flask.jsonify({
            "error": str(e)
        }), e.code
    return flask.jsonify({
        "error": str(e)
    }), 500

# initialize DB
QueuesDAO().init_db()

# development server
if __name__ == "__main__":
    app.run(host="localhost", port=8080)


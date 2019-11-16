#!/usr/bin/env python3

# A simple (primitive) message queue handler in Python
# Not persistent, not transactional. Also, no authentication.
#
# Methods:
#   POST /mq/queue_name/push [ JSON ]        - add a new message to a queue
#   POST /mq/queue_name/pop [ ?n=how_many ]  - remove and get at most n messages from a queue, default is 1
#   GET /mq/queue_name [ ?n=how_many ]       - get at most n messages from a queue, default is 1
#   GET /mq/queue_name/count                 - get a number of messages in a queue
#   PUT /mq/queue_name                       - create a queue if it does not exist
#   DELETE /mq/queue_name                    - delete a queue if it exists
#   GET /mq                                  - get a list of queues
# If there is an error, a { "error": "description" } is returned.

import flask
import werkzeug.exceptions

app = flask.Flask(__name__)

# maximum number of messages user may request for
MAX_MESSAGES_REQUEST_NUMBER = 100

# DAO for working with queues
# Has "in-memory" implementation
class QueuesDAO:
    __queues = {
        "default": []
    }

    # check if a queue (not) exists
    def check_queue(self, qname):
        if qname not in QueuesDAO.__queues:
            raise ValueError("No such queue: '{}'".format(qname))

    # add a message at the beginning
    def push(self, qname, msg):
        self.check_queue(qname)
        QueuesDAO.__queues[qname].insert(0, msg)

    # pop at most n messages from the end
    def pop(self, qname, n=1):
        self.check_queue(qname)
        msgs = []
        i = n
        while QueuesDAO.__queues[qname] and i > 0:
            msgs.append(QueuesDAO.__queues[qname].pop())
            i -= 1
        return msgs

    # get at most n messages from the end.
    # like pop(), but without removing
    def get(self, qname, n=1):
        self.check_queue(qname)
        return QueuesDAO.__queues[qname][-n:]

    # get number of messages in queue
    def count(self, qname):
        self.check_queue(qname)
        return len(QueuesDAO.__queues[qname])

    # create a queue if it not exists
    def create(self, qname):
        if qname in QueuesDAO.__queues:
            return
        QueuesDAO.__queues[qname] = []

    # delete a queue if it exists
    def delete(self, qname):
        if qname not in QueuesDAO.__queues:
            return
        del QueuesDAO.__queues[qname]

    # returns a list of existing queues
    def list(self):
        return list(QueuesDAO.__queues.keys())

# push message to queue
@app.route("/mq/<qname>/push", methods=["POST"])
def mq_push(qname):
    queuesDAO = QueuesDAO()
    data = flask.request.json
    if data is None:
        raise TypeError("Not a JSON value")
    queuesDAO.push(qname, data)
    return flask.jsonify({})

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

# development server
if __name__ == "__main__":
    app.run(host="localhost", port=8080)


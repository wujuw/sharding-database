from flask import Flask
from flask import render_template, url_for
import os
import sys
from multiprocessing import Queue, Process, set_start_method
import threading
import sql_server


def sql_serve(list):
    thread = threading.Thread(target=sql_server.serve, args=(50051, list,))
    thread.start()


app = Flask(__name__)

list = []

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/result/<id>')
def result(id):
    for sql_call in list:
        if sql_call['id'] == str(id):
            print(sql_call)
            return render_template('result.html', sql_call=sql_call)

@app.route('/node')
def node():
    print(list)
    return render_template('node.html', list=list)

if __name__ == '__main__':
    sql_serve(list)
    app.run(host='0.0.0.0', port=8080)
from concurrent import futures
import logging
import sqlite3
import sys
import json

import grpc
import sql_pb2 as sql_pb2
import sql_pb2_grpc as sql_pb2_grpc
import time

class SQLServicer(sql_pb2_grpc.SQLServicer):
    def __init__(self, list):
        self.logger = logging.getLogger(__name__)
        self.list = list
        self.id = 0


    def ExecuteSQL(self, request, context):
        self.logger.info("Got SQL request: %s", request.sql)
        sql_call = {}
        sql_call['sql'] = request.sql
        sql_call['time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        self.id += 1
        sql_call['id'] = str(self.id)
        sql_call['result'] = []
        db = sqlite3.connect("library.db")
        # excute sql and commit
        cursor = db.cursor()
        cursor.execute(request.sql)
        # get columns name
        if cursor.description is not None:
            columns = [column[0] for column in cursor.description]
            sql_call['columns'] = columns
        db.commit()
        # get result
        for row in cursor:
            self.logger.info("fetch row: %s", row)
            sql_call['result'].append(row)
            yield sql_pb2.Record(json=json.dumps(row))
        self.list.append(sql_call)
        cursor.close()
        db.close()

def serve(port, list):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    sql_pb2_grpc.add_SQLServicer_to_server(SQLServicer(list), server)
    server.add_insecure_port('[::]:{}'.format(port))
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig()
    # read arg port
    port = sys.argv[1]
    serve(port)
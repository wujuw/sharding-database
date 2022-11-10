from concurrent import futures
import logging
import sqlite3
import sys
import json

import grpc
import sql_pb2 as sql_pb2
import sql_pb2_grpc as sql_pb2_grpc

class SQLServicer(sql_pb2_grpc.SQLServicer):
    def __init__(self):
        self.logger = logging.getLogger(__name__)


    def ExecuteSQL(self, request, context):
        self.logger.info("Got SQL request: %s", request.sql)
        db = sqlite3.connect("library.db")
        # excute sql and commit
        cursor = db.cursor()
        cursor.execute(request.sql)
        db.commit()
        # get result
        for row in cursor:
            self.logger.info("fetch row: %s", row)
            yield sql_pb2.Record(json=json.dumps(row))
        cursor.close()
        db.close()

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    sql_pb2_grpc.add_SQLServicer_to_server(SQLServicer(), server)
    server.add_insecure_port('[::]:{}'.format(port))
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig()
    # read arg port
    port = sys.argv[1]
    serve(port)
import sys
import logging
import grpc
import sql_pb2 as sql_pb2
import sql_pb2_grpc as sql_pb2_grpc
import json
import yaml
from mo_sql_parsing import format
from mo_sql_parsing import parse

def executeSQL(sql_routes, db_stub_map):
    # 执行sql
    response_map = {}
    for db_name, sql in sql_routes.items():
        recordStream = db_stub_map[db_name].ExecuteSQL(sql_pb2.SQLRequest(sql=sql))
        res = []
        for record in recordStream:
            res.append(json.loads(record.json))
        response_map[db_name] = res

# create table book(id NUMBER PRIMARY KEY, name TEXT)
# {'create table': {'name': 'book', 'columns': [{'name': 'id', 'type': {'number': {}}, 'primary_key': True}, {'name': 'name', 'type': {'text': {}}}]}}
def create_table(sql_json, sharding_rules, db_stub_map):
    # 解析sql
    table_name = sql_json['create table']['name']
    sharding_rule = sharding_rules[table_name]
    if sharding_rule['rule-name'] == 'bind':
        bind_table = sharding_rule['bind-table']
        sharding_rule = sharding_rules[bind_table]
    db_maps = sharding_rule['db-maps']

    # 生成路由 db->sql
    sql_routes = {}
    for db_map in db_maps:
        sql_routes[db_map['db-name']] = format(sql_json)
    
    # 执行sql
    response_map = executeSQL(sql_routes, db_stub_map)
    
    # 归并
    # 对于create table，返回任意一个db的结果即可
    return response_map[db_maps[0]['db-name']]

# drop table book
# {'drop': {'table': 'book'}}
def drop_table(sql_json, sharding_rules, db_stub_map):
    # 解析sql
    table_name = sql_json['drop']['table']
    sharding_rule = sharding_rules[table_name]
    if sharding_rule['rule-name'] == 'bind':
        bind_table = sharding_rule['bind-table']
        sharding_rule = sharding_rules[bind_table]
    db_maps = sharding_rule['db-maps']

    # 生成路由 db->sql
    sql_routes = {}
    for db_map in db_maps:
        sql_routes[db_map['db-name']] = format(sql_json)
    
    # 执行sql
    response_map = executeSQL(sql_routes, db_stub_map)
    
    # 归并
    # 对于drop table，返回任意一个db的结果即可
    return response_map[db_maps[0]['db-name']]

# insert into book(id, name) values(1, 'book1')
# {'columns': ['id', 'name'], 'query': {'select': [{'value': 1}, {'value': {'literal': 'book1'}}]}, 'insert': 'book'}
def insert(sql_json, sharding_rules, db_stub_map):
    # 解析sql
    table_name = sql_json['insert']
    sharding_rule = sharding_rules[table_name]
    rule_name = sharding_rule['rule-name']
    sharding_key = None
    sharding_key_value = None
    if rule_name == 'mod':
        sharding_key = sharding_rule['sharding-key']
        # find sharding_key and index in columns
        columns = sql_json['columns']
        index = columns.index(sharding_key)
        # get sharding_key_value
        sharding_key_value = sql_json['query']['select'][index]['value']
    elif rule_name == 'bind':
        # 替换规则为绑定表的规则
        sharding_key = sharding_rule['bind_key']
        bind_table = sharding_rule['bind-table']
        sharding_rule = sharding_rules[bind_table]
        rule_name = sharding_rule['rule-name']
        # find sharding_key and index in columns
        columns = sql_json['columns']
        index = columns.index(sharding_key)
        # get sharding_key_value
        sharding_key_value = sql_json['query']['select'][index]['value']
    db_maps = sharding_rule['db-maps']

    # 生成路由 db->sql
    sql_routes = {}
    if rule_name == 'mod':
        for db_map in db_maps:
            if sharding_key_value % sharding_rule['mod'] == db_map['remainder']:
                sql_routes[db_map['db-name']] = format(sql_json)
    elif rule_name == 'single':
        sql_routes[db_maps[0]['db-name']] = format(sql_json)
    
    
    # 执行sql
    response_map = executeSQL(sql_routes, db_stub_map)
    
    # 归并
    # 对于insert，只路由到一个db，返回该db的结果即可
    return response_map[db_maps[0]['db-name']]

# delete from book where id=1
# {'where': {'eq': ['id', 1]}, 'delete': 'book'}
def delete(sql_json, sharding_rules, db_stub_map):
    # 解析sql
    table_name = sql_json['delete']
    sharding_rule = sharding_rules[table_name]
    rule_name = sharding_rule['rule-name']
    sharding_key = None
    sharding_key_value = None
    if rule_name == 'mod':
        sharding_key = sharding_rule['sharding-key']
        if 'where' in sql_json:
            # get sharding_key_value
            sharding_key_value = find_sharding_value_from_where(sql_json['where'], sharding_key)
        
    elif rule_name == 'bind':
        # 替换规则为绑定表的规则
        sharding_key = sharding_rule['bind_key']
        bind_table = sharding_rule['bind-table']
        sharding_rule = sharding_rules[bind_table]
        rule_name = sharding_rule['rule-name']
        if 'where' in sql_json:
            # get sharding_key_value
            sharding_key_value = find_sharding_value_from_where(sql_json['where'], sharding_key)
    db_maps = sharding_rule['db-maps']

    # 生成路由 db->sql
    sql_routes = {}
    if rule_name == 'mod':
        if sharding_key_value == None:
            # 生成广播路由
            for db_map in db_maps:
                sql_routes[db_map['db-name']] = format(sql_json)
        else:
            for db_map in db_maps:
                if sharding_key_value % sharding_rule['mod'] == db_map['remainder']:
                    sql_routes[db_map['db-name']] = format(sql_json)
    elif rule_name == 'single':
        sql_routes[db_maps[0]['db-name']] = format(sql_json)
    
    
    # 执行sql
    response_map = executeSQL(sql_routes, db_stub_map)
    
    # 归并
    # 对于delete，只路由到一个db，返回该db的结果即可
    return response_map[db_maps[0]['db-name']]

# only support mod, mod support operator '=' or 'IN': find key=value in where
def find_sharding_value_from_where(where, sharding_key):
    if 'and' in where:
        for sub_where in where['and']:
            value = find_sharding_value_from_where(sub_where, sharding_key)
            if value:
                return value
    elif 'eq' in where and where['eq'][0] == sharding_key:
        return where['eq'][1]
    else:
        return None

# update book set name='book1-updated' where author_id=1
# {'update': 'book', 'set': {'name': {'literal': 'book1-updated'}}, 'where': {'eq': ['author_id', 1]}}
def update(sql_json, sharding_rules, db_stub_map):
    # 解析sql
    table_name = sql_json['update']
    sharding_rule = sharding_rules[table_name]
    rule_name = sharding_rule['rule-name']
    sharding_key = None
    sharding_key_value = None
    if rule_name == 'mod':
        sharding_key = sharding_rule['sharding-key']
        # find sharding_key and index in columns
        if 'where' in sql_json:
            where = sql_json['where']
            sharding_key_value = find_sharding_value_from_where(where, sharding_key)
    elif rule_name == 'bind':
        # 替换规则为绑定表的规则
        sharding_key = sharding_rule['bind_key']
        bind_table = sharding_rule['bind-table']
        sharding_rule = sharding_rules[bind_table]
        rule_name = sharding_rule['rule-name']
        # find sharding_key and index in columns
        if 'where' in sql_json:
            where = sql_json['where']
            sharding_key_value = find_sharding_value_from_where(where, sharding_key)
    db_maps = sharding_rule['db-maps']

    # 生成路由 db->sql
    sql_routes = {}
    if rule_name == 'mod':
        if sharding_key_value == None:
            # 生成广播路由
            for db_map in db_maps:
                sql_routes[db_map['db-name']] = format(sql_json)
        else:
            for db_map in db_maps:
                if sharding_key_value % sharding_rule['mod'] == db_map['remainder']:
                    sql_routes[db_map['db-name']] = format(sql_json)
    elif rule_name == 'single':
        sql_routes[db_maps[0]['db-name']] = format(sql_json)
    
    
    # 执行sql
    response_map = executeSQL(sql_routes, db_stub_map)
    
    # 归并
    response_list = []
    for db_name, response in response_map.items():
        response_list.append(response)
    return response_list

# select * from book where author_id=1
# {'select': '*', 'from': 'book', 'where': {'eq': ['author_id', 1]}}
def select(sql_json, sharding_rules, db_stub_map):
    # 解析sql
    table_name = sql_json['from']
    sharding_rule = sharding_rules[table_name]
    rule_name = sharding_rule['rule-name']
    sharding_key = None
    sharding_key_value = None
    if rule_name == 'mod':
        sharding_key = sharding_rule['sharding-key']
        # find sharding_key and index in columns
        if 'where' in sql_json:
            where = sql_json['where']
            sharding_key_value = find_sharding_value_from_where(where, sharding_key)
    elif rule_name == 'bind':
        # 替换规则为绑定表的规则
        sharding_key = sharding_rule['bind_key']
        bind_table = sharding_rule['bind-table']
        sharding_rule = sharding_rules[bind_table]
        rule_name = sharding_rule['rule-name']
        # find sharding_key and index in columns
        if 'where' in sql_json:
            where = sql_json['where']
            sharding_key_value = find_sharding_value_from_where(where, sharding_key)
    db_maps = sharding_rule['db-maps']

    # 生成路由 db->sql
    sql_routes = {}
    if rule_name == 'mod':
        if sharding_key_value == None:
            # 生成广播路由
            for db_map in db_maps:
                sql_routes[db_map['db-name']] = format(sql_json)
        else:
            for db_map in db_maps:
                if sharding_key_value % sharding_rule['mod'] == db_map['remainder']:
                    sql_routes[db_map['db-name']] = format(sql_json)
    elif rule_name == 'single':
        sql_routes[db_maps[0]['db-name']] = format(sql_json)
    
    
    # 执行sql
    response_map = executeSQL(sql_routes, db_stub_map)
    
    # 归并
    response_list = []
    for db_name, response in response_map.items():
        response_list.append(response)
    return response_list

if __name__ == '__main__':
    logging.basicConfig()

    datasources_path = sys.argv[1]
    sharding_rules_path = sys.argv[2]
    
    # read config
    with open(datasources_path, 'r') as f:
        datasources = yaml.safe_load(f)['datasources']
    with open(sharding_rules_path, 'r') as f:
        sharding_rules = yaml.safe_load(f)['sharding-rules']
    
    # connect to dbs
    db_client_map = {}
    for datasource in datasources:
        db_client_map[datasource['name']] = sql_pb2_grpc.SQLStub(grpc.insecure_channel('{}:{}'.format(datasource['host'], datasource['port'])))
    
    # read sql
    while True:
        sql = input("sql> ")
        sql_json = parse(sql)
        if 'insert' in sql_json:
            res = insert(sql_json, sharding_rules, db_client_map)
        elif 'delete' in sql_json:
            res = delete(sql_json, sharding_rules, db_client_map)
        elif 'update' in sql_json:
            res = update(sql_json, sharding_rules, db_client_map)
        elif 'select' in sql_json:
            res = select(sql_json, sharding_rules, db_client_map)
        print(res)


# create table author(id NUMBER, name TEXT)
# create table book(id NUMBER, name TEXT, author_id NUMBER)
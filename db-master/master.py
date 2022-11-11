import copy
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
        print('execute sql use {}: {}'.format(db_name, sql))
        recordStream = db_stub_map[db_name].ExecuteSQL(sql_pb2.SQLRequest(sql=sql))
        res = []
        for record in recordStream:
            res.append(json.loads(record.json))
        response_map[db_name] = res
    return response_map

# create table book(id NUMBER PRIMARY KEY, name TEXT)
# {'create table': {'name': 'book', 'columns': [{'name': 'id', 'type': {'number': {}}, 'primary_key': True}, {'name': 'name', 'type': {'text': {}}}]}}
def create_table(sql, sql_json, sharding_rules, db_stub_map):
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
        sql_routes[db_map['db-name']] = sql
    
    # 执行sql
    response_map = executeSQL(sql_routes, db_stub_map)
    
    # 归并
    # 对于create table，返回任意一个db的结果即可
    for res in response_map.values():
        return res

# drop table book
# {'drop': {'table': 'book'}}
def drop_table(sql, sql_json, sharding_rules, db_stub_map):
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
        sql_routes[db_map['db-name']] = sql
    
    # 执行sql
    response_map = executeSQL(sql_routes, db_stub_map)
    
    # 归并
    # 对于drop table，返回任意一个db的结果即可
    for res in response_map.values():
        return res

# insert into book(id, name) values(1, 'book1')
# {'columns': ['id', 'name'], 'query': {'select': [{'value': 1}, {'value': {'literal': 'book1'}}]}, 'insert': 'book'}
def insert(sql, sql_json, sharding_rules, db_stub_map):
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
        sharding_key = sharding_rule['bind-key']
        bind_table = sharding_rule['bind-table']
        sharding_rule = sharding_rules[bind_table]
        rule_name = sharding_rule['rule-name']
        # find sharding_key and index in columns
        columns = sql_json['columns']
        index = columns.index(sharding_key)
        # get sharding_key_value
        sharding_key_value = sql_json['query']['select'][index]['value']
    elif rule_name == 'lang':
        # find sharding_key and index in columns
        sharding_key = sharding_rule['sharding-key']
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
                sql_routes[db_map['db-name']] = sql
    elif rule_name == 'single':
        sql_routes[db_maps[0]['db-name']] = sql
    elif rule_name == 'lang':
        sql_routes = sql_routes_lang(sharding_key_value, db_maps)
    
    
    # 执行sql
    response_map = executeSQL(sql_routes, db_stub_map)
    
    # 归并
    # 对于insert，只路由到一个db，返回该db的结果即可
    for res in response_map.values():
        return res

def sql_routes_lang(sharding_value, db_maps):
    sql_routes = {}
    for db_map in db_maps:
        if sharding_value['literal'] == db_map['lang']:
            sql_routes[db_map['db-name']] = sql
    return sql_routes

# delete from book where id=1
# {'where': {'eq': ['id', 1]}, 'delete': 'book'}
def delete(sql, sql_json, sharding_rules, db_stub_map):
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
        sharding_key = sharding_rule['bind-key']
        bind_table = sharding_rule['bind-table']
        sharding_rule = sharding_rules[bind_table]
        rule_name = sharding_rule['rule-name']
        if 'where' in sql_json:
            # get sharding_key_value
            sharding_key_value = find_sharding_value_from_where(sql_json['where'], sharding_key)
    elif rule_name == 'lang':
        sharding_key = sharding_rule['sharding-key']
        # get sharding_key_value
        if 'where' in sql_json:
            sharding_key_value = find_sharding_value_from_where(sql_json['where'], sharding_key)
    db_maps = sharding_rule['db-maps']

    # 生成路由 db->sql
    sql_routes = {}
    if rule_name == 'mod':
        if sharding_key_value == None:
            # 生成广播路由
            for db_map in db_maps:
                sql_routes[db_map['db-name']] = sql
        else:
            for db_map in db_maps:
                if sharding_key_value % sharding_rule['mod'] == db_map['remainder']:
                    sql_routes[db_map['db-name']] = sql
    elif rule_name == 'single':
        sql_routes[db_maps[0]['db-name']] = sql
    elif rule_name == 'lang':
        sql_routes = sql_routes_lang(sharding_key_value, db_maps)
    
    
    # 执行sql
    response_map = executeSQL(sql_routes, db_stub_map)
    
    # 归并
    # 对于delete，只路由到一个db，返回该db的结果即可
    for res in response_map.values():
        return res

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
def update(sql, sql_json, sharding_rules, db_stub_map):
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
        sharding_key = sharding_rule['bind-key']
        bind_table = sharding_rule['bind-table']
        sharding_rule = sharding_rules[bind_table]
        rule_name = sharding_rule['rule-name']
        # find sharding_key and index in columns
        if 'where' in sql_json:
            where = sql_json['where']
            sharding_key_value = find_sharding_value_from_where(where, sharding_key)
    elif rule_name == 'lang':
        sharding_key = sharding_rule['sharding-key']
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
                sql_routes[db_map['db-name']] = sql
        else:
            for db_map in db_maps:
                if sharding_key_value % sharding_rule['mod'] == db_map['remainder']:
                    sql_routes[db_map['db-name']] = sql
    elif rule_name == 'single':
        sql_routes[db_maps[0]['db-name']] = sql
    else:
        sql_routes = get_sql_routes_by_rule(sharding_rule, sharding_key_value)
    
    # 执行sql
    response_map = executeSQL(sql_routes, db_stub_map)
    
    # 归并
    response_list = []
    for db_name, response in response_map.items():
        response_list.append(response)
    return response_list

def get_sql_routes_by_rule(sharding_rule, sharding_key_value):
    rule_name = sharding_rule['rule-name']
    db_maps = sharding_rule['db-maps']
    sql_routes = {}
    if rule_name == 'mod':
        if sharding_key_value == None:
            # 生成广播路由
            for db_map in db_maps:
                sql_routes[db_map['db-name']] = sql
        else:
            for db_map in db_maps:
                if sharding_key_value % sharding_rule['mod'] == db_map['remainder']:
                    sql_routes[db_map['db-name']] = sql
    elif rule_name == 'single':
        sql_routes[db_maps[0]['db-name']] = sql
    elif rule_name == 'lang':
        sql_routes = sql_routes_lang(sharding_key_value, db_maps)
    return sql_routes

# select * from book where author_id=1
# {'select': '*', 'from': 'book', 'where': {'eq': ['author_id', 1]}}
def select(sql, sql_json, sharding_rules, db_stub_map):
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
        sharding_key = sharding_rule['bind-key']
        bind_table = sharding_rule['bind-table']
        sharding_rule = sharding_rules[bind_table]
        rule_name = sharding_rule['rule-name']
        # find sharding_key and index in columns
        if 'where' in sql_json:
            where = sql_json['where']
            sharding_key_value = find_sharding_value_from_where(where, sharding_key)
    elif rule_name == 'lang':
        sharding_key = sharding_rule['sharding-key']
        # find sharding_key and index in columns
        if 'where' in sql_json:
            where = sql_json['where']
            sharding_key_value = find_sharding_value_from_where(where, sharding_key)
    db_maps = sharding_rule['db-maps']
    fields = getFields(sql_json, db_client_map[db_maps[0]['db-name']])

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
    elif rule_name == 'lang':
        sql_routes = get_sql_routes_by_rule(sharding_rule, sharding_key_value)
    
    # 若路由到单节点， 则无需改写sql，无需归并
    if len(sql_routes) == 1:
        print("路由到单节点")
        response_list = []
        response_list.append(fields)
        for db_name, sql in sql_routes.items():
            res_map = executeSQL(sql_routes, db_stub_map)
            for db_name, response in res_map.items():
                for row in response:
                    response_list.append(row)
        return response_list

    # 改写sql，加上字段
    addIndex = addOrderByFields(sql_json, fields)
    rewriteRes = rewrite_limit(sql_json)
    for key, sql in sql_routes.items():
        sql_routes[key] = format(sql_json)
    
    # 执行sql
    response_map = executeSQL(sql_routes, db_stub_map)
    
    # 归并
    response_list = []
    for db_name, response in response_map.items():
        for row in response:
            response_list.append(row)
    response_list = checkOrderBy(response_list, sql_json, fields)
    response_list = checkLimit(response_list, sql_json, rewriteRes)
    response_list.insert(0, fields)
    # 删除多余的字段
    if addIndex != None:
        for row in response_list:
            del row[addIndex:]
    return response_list

def rewrite_limit(sql_json):
    if 'orderby' in sql_json:
        if 'offset' in sql_json:
            offset = sql_json['offset']
            if 'limit' in sql_json:
                limit = sql_json['limit']
                sql_json['limit'] = offset + limit
                sql_json['offset'] = 0
                return [limit, offset]
    return None

def checkLimit(response_list, sql_json, rewriteRes):
    limit = -1
    offset = -1
    if rewriteRes != None:
        sql_json['limit'] = rewriteRes[0]
        sql_json['offset'] = rewriteRes[1]
    if 'limit' in sql_json:
        limit = sql_json['limit']
    if 'offset' in sql_json:
        offset = sql_json['offset']
    if limit == -1:
        return response_list
    if offset == -1:
        return response_list[:limit]
    return response_list[offset:offset+limit]

def checkOrderBy(response_list, sql_json, fields):
    if 'orderby' in sql_json:
        order_by = sql_json['orderby']
        order_by_field = order_by['value']
        order_by_index = fields.index(order_by_field)
        order_by_type = 'asc'
        if 'sort' in order_by:
            order_by_type = order_by['sort']
        if order_by_type == 'asc':
            response_list.sort(key=lambda x: x[order_by_index])
        else:
            response_list.sort(key=lambda x: x[order_by_index], reverse=True)
    return response_list

def addOrderByFields(sql_json, fields):
    addIndex = None
    if 'orderby' in sql_json:
        order_by = sql_json['orderby']
        if order_by['value'] not in fields:
            fields.append(order_by['value'])
            addIndex = len(fields) - 1
        fields_in_json = []
        for field in fields:
            fields_in_json.append({'value': field})
        sql_json['select'] = fields_in_json
    return addIndex

def getFields(sql_json, db_stub):
    if 'select' in sql_json:
        fields = []
        if isinstance(sql_json['select'], list):
            for field in sql_json['select']:
                fields.append(field['value'])
        else:
            if sql_json['select'] == '*':
                return getMetadataFromTable(sql_json['from'], db_stub)['columns']
            else:
                fields.append(sql_json['select']['value'])
        return fields
    else:
        return []

def getMetadataFromTable(table_name, db_stub):
    metadata = {}
    sql = "select * from sqlite_master where type='table' and name='{}'".format(table_name)
    recordsRes = db_stub.ExecuteSQL(sql_pb2.SQLRequest(sql=sql))
    for recordRes in recordsRes:
        record = json.loads(recordRes.json)
        metadata['table_name'] = record[1]
        metadata['sql'] = record[4]
        metadata['columns_json'] = parse(record[4])['create table']['columns']
        metadata['columns'] = []
        for column in metadata['columns_json']:
            metadata['columns'].append(column['name'])
            if 'primary-key' in column and column['primary-key'] == True:
                metadata['primary-key'] = column['name']
    return metadata

def sharding_value_lang(sql_json):
    value = find_sharding_value_from_where(sql_json['where'], 'lang')

# "select * from a join b on a.id = b.id"
# {'select': '*', 'from': ['a', {'join': 'b', 'on': {'eq': ['a.id', 'b.id']}}]}
def join(sql, sql_json, sharding_rules, db_client_map):
    # 获取join的表名
    join_table_name = sql_json['from'][1]['join']
    # 获取join的字段
    join_on = sql_json['from'][1]['on']
    join_on_field = {'a': join_on['eq'][0], 'b': join_on['eq'][1]}
    
    sql_json_a = {}
    sql_json_b = {}
    sql_json_a['from'] = sql_json['from'][0]
    sql_json_b['from'] = join_table_name

    if sharding_rules[sql_json_a['from']]['rule-name'] == 'bind':
        bind_table_name = sharding_rules[sql_json_a['from']]['bind-table']
        sharding_rules[sql_json_a['from']] = sharding_rules[bind_table_name]
    if sharding_rules[sql_json_b['from']]['rule-name'] == 'bind':
        bind_table_name = sharding_rules[sql_json_b['from']]['bind-table']
        sharding_rules[sql_json_b['from']] = sharding_rules[bind_table_name]
    metadata_a = getMetadataFromTable(sql_json_a['from'], db_client_map[sharding_rules[sql_json_a['from']]['db-maps'][0]['db-name']])
    metadata_b = getMetadataFromTable(sql_json_b['from'], db_client_map[sharding_rules[sql_json_b['from']]['db-maps'][0]['db-name']])

    ab = {'a': {'json': sql_json_a, 'metadata': metadata_a}, 'b': {'json': sql_json_b, 'metadata': metadata_b}}
    fields = {'a':[], 'b':[]}
    table_name = {'a': sql_json_a['from'], 'b': sql_json_b['from']}
    for key in ab:
        if sql_json['select'] == '*':
            ab[key]['json']['select'] = []
            for column in ab[key]['metadata']['columns_json']:
                ab[key]['json']['select'].append({'value': table_name[key] + '.' + column['name']})
                fields[key].append(table_name[key] + '.' + column['name'])
        else:
            ab[key]['json']['select'] = []
            for field in sql_json['select']:
                if field['value'] in ab[key]['metadata']['columns']:
                    ab[key]['json']['select'].append({'value': table_name[key] + '.' + field['value']})
                    fields[key].append(table_name[key] + '.' + field['value'])
                elif field['value'] == key + '.*':
                    for column in ab[key]['metadata']['columns']:
                        ab[key]['json']['select'].append({'value': table_name[key] + '.' + column})
                        fields[key].append(table_name[key] + '.' + column)
        if join_on_field[key] not in fields[key] and join_on_field[key].split('.')[1] not in fields[key]:
            fields[key].append(join_on_field)
            ab[key]['json']['select'].append({'value': join_on_field})
        if 'where' in sql_json:
            conditions = get_conditions_from_where(sql_json['json']['where'], ab[key]['json']['from'], ab[key]['metadata']['columns'])
            ab[key]['json']['where'] = {'and': conditions}
        if 'orderby' in sql_json:
            orderby_value = sql_json['orderby']['value']
            if orderby_value in fields[key]:
                ab[key]['json']['orderby'] = {'value': orderby_value}

    res_a = select(sql, ab['a']['json'], sharding_rules, db_client_map)[1:]
    res_b = select(sql, ab['b']['json'], sharding_rules, db_client_map)[1:]
    res = []
    for a in res_a:
        for b in res_b:
            if a[fields['a'].index(join_on_field['a'])] == b[fields['b'].index(join_on_field['b'])]:
                res.append(a + b)

    checkOrderBy(sql_json, fields, res)
    res.insert(0, fields['a'] + fields['b'])
    return res

def get_conditions_from_where(where, table_name, columns):
    conditions = []
    if 'and' in where:
        for condition in where['and']:
            conditions.append(get_conditions_from_where(condition, table_name, columns))
    elif 'or' in where:
        for condition in where['or']:
            conditions.append(get_conditions_from_where(condition, table_name, columns))
    elif 'eq' in where:
        if where['eq'][0].split('.')[0] == table_name:
            if where['eq'][0].split('.')[1] in columns:
                conditions.append(where)
    elif 'neq' in where:
        if where['neq'][0].split('.')[0] == table_name:
            if where['neq'][0].split('.')[1] in columns:
                conditions.append(where)
    elif 'gt' in where:
        if where['gt'][0].split('.')[0] == table_name:
            if where['gt'][0].split('.')[1] in columns:
                conditions.append(where)
    elif 'gte' in where:
        if where['gte'][0].split('.')[0] == table_name:
            if where['gte'][0].split('.')[1] in columns:
                conditions.append(where)
    elif 'lt' in where:
        if where['lt'][0].split('.')[0] == table_name:
            if where['lt'][0].split('.')[1] in columns:
                conditions.append(where)
    elif 'lte' in where:
        if where['lte'][0].split('.')[0] == table_name:
            if where['lte'][0].split('.')[1] in columns:
                conditions.append(where)
    elif 'like' in where:
        if where['like'][0].split('.')[0] == table_name:
            if where['like'][0].split('.')[1] in columns:
                conditions.append(where)
    elif 'not-like' in where:
        if where['not-like'][0].split('.')[0] == table_name:
            if where['not-like'][0].split('.')[1] in columns:
                conditions.append(where)
    elif 'in' in where:
        if where['in'][0].split('.')[0] == table_name:
            if where['in'][0].split('.')[1] in columns:
                conditions.append(where)
    elif 'not-in' in where:
        if where['not-in'][0].split('.')[0] == table_name:
            if where['not-in'][0].split('.')[1] in columns:
                conditions.append(where)
    return conditions

if __name__ == '__main__':
    logging.basicConfig()

    # datasources_path = sys.argv[1]
    # sharding_rules_path = sys.argv[2]
    datasources_path = './db-master/datasources.yaml'
    sharding_rules_path = './db-master/sharding-rules.yaml'
    
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
        res = None
        if 'insert' in sql_json:
            res = insert(sql, sql_json, sharding_rules, db_client_map)
        elif 'delete' in sql_json:
            res = delete(sql, sql_json, sharding_rules, db_client_map)
        elif 'update' in sql_json:
            res = update(sql, sql_json, sharding_rules, db_client_map)
        elif 'select' in sql_json and not isinstance(sql_json['from'], list):
            res = select(sql, sql_json, sharding_rules, db_client_map)
        elif 'create table' in sql_json:
            res = create_table(sql, sql_json, sharding_rules, db_client_map)
        elif 'drop' in sql_json:
            res = drop_table(sql, sql_json, sharding_rules, db_client_map)
        elif isinstance(sql_json['from'], list) and 'join' in sql_json['from'][1]:
            res = join(sql, sql_json, sharding_rules, db_client_map)

        if isinstance(res, list):
            for row in res:
                print(row)
        else:
            print(res)



# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: sql.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\tsql.proto\x12\x0b\x64istributed\"\x19\n\nSQLRequest\x12\x0b\n\x03sql\x18\x01 \x01(\t\"\x16\n\x06Record\x12\x0c\n\x04json\x18\x01 \x01(\t2C\n\x03SQL\x12<\n\nExecuteSQL\x12\x17.distributed.SQLRequest\x1a\x13.distributed.Record0\x01\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'sql_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _SQLREQUEST._serialized_start=26
  _SQLREQUEST._serialized_end=51
  _RECORD._serialized_start=53
  _RECORD._serialized_end=75
  _SQL._serialized_start=77
  _SQL._serialized_end=144
# @@protoc_insertion_point(module_scope)

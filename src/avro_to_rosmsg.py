#!/usr/bin/python

import sys

import rospy
import rosbridge_library.internal.ros_loader as ros_loader

import json

import avro.schema

#raw_schema = open(sys.argv[1])
#json_schema = json.load(raw_schema)

def convert_avro_name_to_ros_name(name):
    if name == 'boolean':
        return 'bool'
    if name == 'int':
        return 'int32'
    if name == 'long':
        return 'int64'
    if name == 'float':
        return 'float32'
    if name == 'double':
        return 'float64'
    if name == 'bytes':
        return 'byte[]'
    if name == 'string':
        return 'string'
    return ''

def process_union_schema(schema):
    union_type = []
    for sc in schema.schemas:
        if sc.fullname != 'null':
            union_type.append(sc.fullname)
    if len(union_type) >= 2 or len(union_type) == 0:
        print 'Error!'
    return convert_avro_name_to_ros_name(union_type[0])

def process_enum_schema(schema):
    symbols = ''
    for s in schema.symbols:
        symbols += 'string ' + s + '=' + s + '\n'
    return symbols

def process_record_schema(schema, outfile=None):
    schemas_to_parse = []
    for field_key in schema.fields_dict:
        field = schema.fields_dict[field_key]
        if type(field.type) == avro.schema.PrimitiveSchema:
            line = convert_avro_name_to_ros_name(field.type.fullname) + ' ' + field.name
            print line
            outfile.write(line + '\n')
        elif type(field.type) == avro.schema.ArraySchema:
            line = field.type.items.name + '[] ' + field.name
            print line
            outfile.write(line + '\n')
            schemas_to_parse.append(field.type.items)
        elif type(field.type) == avro.schema.UnionSchema:
            line = process_union_schema(field.type) + ' ' + field.name
            print line
            outfile.write(line + '\n')
        elif type(field.type) == avro.schema.EnumSchema:
            line = process_enum_schema(field.type)
            line += 'string ' + field.name + '\n'
            print line
            outfile.write(line + '\n')
        else:
            line = field.type.name + ' ' + field.name
            print line
            outfile.write(line + '\n')
            schemas_to_parse.append(field.type)
    return schemas_to_parse

schemas = [avro.schema.parse(open(sys.argv[1]).read())]
while len(schemas) != 0:
    new_schemas = []
    for schema in schemas:
        msg_file = open(schema.name + '.msg', 'wt')
        ns = process_record_schema(schema, msg_file)
        print ''
        new_schemas.extend(ns)
        msg_file.close()
    schemas = new_schemas


#!/usr/bin/python

import sys

import rospy
import rosbridge_library.internal.ros_loader as ros_loader

import json

import avro.schema

#raw_schema = open(sys.argv[1])
#json_schema = json.load(raw_schema)

def process_union_schema(schema):
    union_type = []
    for sc in schema.schemas:
        if sc.fullname != 'null':
            union_type.append(sc.fullname)
    if len(union_type) >= 2 or len(union_type) == 0:
        print 'Error!'
    return union_type[0]

def process_record_schema(schema):
    fields_to_parse = []
#    print 'name:', schema.name
#    print '======'
    for field_key in schema.fields_dict:
        field = schema.fields_dict[field_key]
        if type(field.type) == avro.schema.PrimitiveSchema:
            print field.type.fullname, field.name
        elif type(field.type) == avro.schema.ArraySchema:
            print field.type.items.name + '[]', field.name
            fields_to_parse.append(field.type.items)
        elif type(field.type) == avro.schema.UnionSchema:
            print process_union_schema(field.type), field.name
        else:
            print field.type.name, field.name
            fields_to_parse.append(field.type)
    return fields_to_parse

schemas = [avro.schema.parse(open(sys.argv[1]).read())]
while len(schemas) != 0:
    new_schemas = []
    for schema in schemas:
        ns = process_record_schema(schema)
        print ''
        new_schemas.extend(ns)
    schemas = new_schemas



#fields_to_parse = []
#for field_key in schema.fields_dict:
#    field = schema.fields_dict[field_key]
#    if type(field.type) == avro.schema.PrimitiveSchema:
#        print field.type.fullname, field.name
#    else:
#        print type(field.type), field.type.name, field.name
#        fields_to_parse.append(field)
#print
#print fields_to_parse[0].name, ":"
#field_array = None
#for field_key in fields_to_parse[0].type.fields_dict:
#    field = fields_to_parse[0].type.fields_dict[field_key]
#    if type(field.type) == avro.schema.PrimitiveSchema:
#        print field.type.fullname, field.name
#    elif type(field.type) == avro.schema.ArraySchema:
#        field_array = field
#        print field.type.items.name + '[]', field.name
#    else:
#        print field.type.name, field.name
#

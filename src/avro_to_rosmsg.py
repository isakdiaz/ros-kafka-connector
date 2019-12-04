#!/usr/bin/python

import sys

import rospy
import rosbridge_library.internal.ros_loader as ros_loader

import json

import avro.schema

#raw_schema = open(sys.argv[1])
#json_schema = json.load(raw_schema)

schema = avro.schema.parse(open(sys.argv[1]).read())
for field_key in schema.fields_dict:
    field = schema.fields_dict[field_key]
    if type(field.type) == avro.schema.PrimitiveSchema:
        print field.type.fullname, field.name
    else:
        print field.type.name, field.name

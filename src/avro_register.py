#!/usr/bin/env python

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from confluent.schemaregistry.client import CachedSchemaRegistryClient
import sys
from pprint import pprint

print 'registering schema: ' + sys.argv[1] + ', to name: ' + sys.argv[2] + ', in server: ' + sys.argv[3]
schema = avro.schema.parse(open(sys.argv[1], "rb").read())
print schema
client = CachedSchemaRegistryClient(url=sys.argv[3])
client.register(sys.argv[2], schema)

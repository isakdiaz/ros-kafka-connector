#!/usr/bin/env python

import rospy
from rospy_message_converter import message_converter
from rospy_message_converter import json_message_converter
import rosbridge_library.internal.ros_loader as ros_loader

from kafka import KafkaProducer
from kafka import KafkaConsumer
from confluent.schemaregistry.client import CachedSchemaRegistryClient
from confluent.schemaregistry.serializers import MessageSerializer

class kafka_publish():

    def __init__(self):

        # initialize node
        rospy.init_node("kafka_publish")
        rospy.on_shutdown(self.shutdown)

        # Retrieve parameters from launch file
        bootstrap_server = rospy.get_param("~bootstrap_server", "localhost:9092")
        schema_server = rospy.get_param("~schema_server", "http://localhost:8081")

        self.ros_topic = rospy.get_param("~ros_topic", "test")
        self.msg_type = rospy.get_param("~msg_type", "std_msgs/String")
        self.kafka_topic = rospy.get_param("~kafka_topic", "bar")
        self.avro_subject = rospy.get_param("~avro_subject", "bar-value")

        # Create schema registry connection and serializer
        self.client = CachedSchemaRegistryClient(url=schema_server)
        self.serializer = MessageSerializer(self.client)
        _, self.avro_schema, _ = self.client.get_latest_schema(self.avro_subject)

        # Create kafka producer
        # TODO: check possibility of using serializer directly (param value_serializer from KafkaProducer)
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_server)

        # ROS does not allow a change in msg type once a topic is created. Therefore the msg
        # type must be imported and specified ahead of time.
        msg_func = ros_loader.get_message_class(self.msg_type)

        # Subscribe to the topic with the chosen imported message type
        rospy.Subscriber(self.ros_topic, msg_func, self.callback)
        rospy.logwarn("Using {} MSGs from ROS: {} -> KAFKA: {}".format(self.msg_type, self.ros_topic,self.kafka_topic))


    def callback(self, msg):
        # Output msg to ROS and send to Kafka server
        rospy.logwarn("MSG Received: {}".format(msg))
        # Convert from ROS Msg to Dictionary
        msg_as_dict = message_converter.convert_ros_message_to_dictionary(msg)
        # also print as json for debugging purposes
        msg_as_json = json_message_converter.convert_ros_message_to_json(msg)
        rospy.logwarn(msg_as_json)
        # Convert from Dictionary to Kafka message
        # this way is slow, as it has to retrieve last schema
        # msg_as_serial = self.serializer.encode_record_for_topic(self.kafka_topic, msg_as_dict)
        msg_as_serial = self.serializer.encode_record_with_schema(self.kafka_topic, self.avro_schema, msg_as_dict)
        self.producer.send(self.kafka_topic, value=msg_as_serial)


    def run(self):
        rate = rospy.Rate(10)
        while not rospy.is_shutdown():
            rate.sleep()

    def shutdown(self):
        rospy.loginfo("Shutting down")

if __name__ == "__main__":

    try:
        node = kafka_publish()
        node.run()
    except rospy.ROSInterruptException:
        pass

    rospy.loginfo("Exiting")

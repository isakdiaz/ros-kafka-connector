#!/usr/bin/env python

import rospy
import json

from kafka import KafkaProducer
from kafka import KafkaConsumer
from rospy_message_converter import json_message_converter
from rospy_message_converter import message_converter

import rosbridge_library.internal.ros_loader as ros_loader

from confluent.schemaregistry.client import CachedSchemaRegistryClient
from confluent.schemaregistry.serializers import MessageSerializer


from pprint import pprint

class ros_publish():

    def __init__(self):

        # initialize node
        rospy.init_node("ros_publish")
        rospy.on_shutdown(self.shutdown)

        # Retrieve parameters from launch file
        bootstrap_server = rospy.get_param("~bootstrap_server", "localhost:9092")
        self.ros_topic = rospy.get_param("~ros_topic", "test")
        self.kafka_topic = rospy.get_param("~kafka_topic", "bar")
        self.msg_type = rospy.get_param("~msg_type", "std_msgs/String")

        self.client = CachedSchemaRegistryClient(url='http://localhost:8081')
        self.serializer = MessageSerializer(self.client)

        # Create kafka consumer
        self.consumer = KafkaConsumer(self.kafka_topic,
                                    bootstrap_servers=bootstrap_server,
                                    #value_deserializer=lambda m: json.loads(m.decode('ascii')),
                                    auto_offset_reset='earliest',
                                    consumer_timeout_ms=5000)

        # Import msg type
        msg_func = ros_loader.get_message_class(self.msg_type)

        # Subscribe to ROS topic of interest
        self.publisher = rospy.Publisher(self.ros_topic, msg_func, queue_size=10)
        rospy.logwarn("Using {} MSGs from KAFKA: {} -> ROS: {}".format(self.msg_type, self.kafka_topic,self.ros_topic))

    def run(self):
        while not rospy.is_shutdown():
            for msg in self.consumer:
                #pprint(msg)
                #pprint(msg.value)
                pprint(self.serializer.decode_message(msg.value))
                # Convert Kafka message to Dictionary
                msg_as_dict = self.serializer.decode_message(msg.value) #json.dumps(msg.value)
                ros_msg = message_converter.convert_dictionary_to_ros_message(self.msg_type, msg_as_dict)
                # Publish to ROS topic
                self.publisher.publish(ros_msg)
                rospy.logwarn("Received MSG:") #.format(ros_msg))
        rospy.spin()


    def shutdown(self):
        rospy.loginfo("Shutting down")

if __name__ == "__main__":

    try:
        node = ros_publish()
        node.run()
    except rospy.ROSInterruptException:
        pass

    rospy.loginfo("Exiting")

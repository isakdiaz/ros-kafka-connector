#!/usr/bin/env python

import rospy
from rospy_message_converter import message_converter

import rosbridge_library.internal.ros_loader as ros_loader

from kafka import KafkaProducer
from kafka import KafkaConsumer

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
        schema_server = rospy.get_param("~schema_server", "http://localhost:8081")
        self.ros_topic = rospy.get_param("~ros_topic", "test")
        self.kafka_topic = rospy.get_param("~kafka_topic", "bar")
        self.msg_type = rospy.get_param("~msg_type", "std_msgs/String")
        
        self.use_ssl = rospy.get_param("~use_ssl", False)

        if (self.use_ssl):
            self.ssl_cafile = rospy.get_param("~ssl_cafile", '../include/certificate.pem')
            self.ssl_keyfile = rospy.get_param("~ssl_keyfile", "../include/kafka.client.keystore.jks")
            self.ssl_password = rospy.get_param("~ssl_password", "password")
            self.ssl_security_protocol = rospy.get_param("~ssl_security_protocol", "SASL_SSL")
            self.ssl_sasl_mechanism = rospy.get_param("~ssl_sasl_mechanism", "PLAIN")
            self.sasl_plain_username = rospy.get_param("~sasl_plain_username", "username")
            self.sasl_plain_password = rospy.get_param("~sasl_plain_password", "password")

        # Create schema registry connection and serializer
        self.client = CachedSchemaRegistryClient(url=schema_server)
        self.serializer = MessageSerializer(self.client)

        # Create kafka consumer
        # TODO: check possibility of using serializer directly (param value_deserializer from KafkaConsumer)
        if(self.use_ssl):
            self.consumer = KafkaConsumer(self.kafka_topic,
                                        bootstrap_servers=bootstrap_server,
                                        security_protocol=self.ssl_security_protocol,
                                        ssl_check_hostname=False,
                                        ssl_cafile=self.ssl_cafile,
                                        ssl_keyfile=self.ssl_keyfile,
                                        sasl_mechanism=self.ssl_sasl_mechanism,
                                        ssl_password=self.ssl_password,
                                        sasl_plain_username=self.sasl_plain_username,
                                        sasl_plain_password=self.sasl_plain_password
                                        )
        else:
			self.consumer = KafkaConsumer(self.kafka_topic,
                                bootstrap_servers=bootstrap_server,
                                auto_offset_reset='latest',
                                consumer_timeout_ms=5000)

        # Import msg type
        msg_func = ros_loader.get_message_class(self.msg_type)

        # Subscribe to ROS topic of interest
        self.publisher = rospy.Publisher(self.ros_topic, msg_func, queue_size=10)
        rospy.logwarn("Using {} MSGs from KAFKA: {} -> ROS: {}".format(self.msg_type, self.kafka_topic,self.ros_topic))

    def run(self):
        while not rospy.is_shutdown():
            for msg in self.consumer:
                rospy.logwarn("Received MSG from: " + self.kafka_topic)
                # Convert Kafka message to Dictionary
                msg_as_dict = self.serializer.decode_message(msg.value)
                # Convert Dictionary to ROS Msg
                ros_msg = message_converter.convert_dictionary_to_ros_message(self.msg_type, msg_as_dict)
                # Publish to ROS topic
                self.publisher.publish(ros_msg)

    def shutdown(self):
        rospy.loginfo("Shutting down")

if __name__ == "__main__":

    try:
        node = ros_publish()
        node.run()
    except rospy.ROSInterruptException:
        pass

    rospy.loginfo("Exiting")

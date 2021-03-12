#!/usr/bin/env python

import rospy
from rospy_message_converter import message_converter
from rospy_message_converter import json_message_converter

import rosbridge_library.internal.ros_loader as ros_loader

from kafka import KafkaProducer
from kafka import KafkaConsumer

from confluent.schemaregistry.client import CachedSchemaRegistryClient
from confluent.schemaregistry.serializers import MessageSerializer


from pprint import pprint

class Topic:
    def __init__(self, ros_topic, kafka_topic, ros_msg_type):
        self.ros_topic = ros_topic
        self.kafka_topic = kafka_topic
        self.ros_msg_type = ros_msg_type


class ros_publish():

    def __init__(self):

        # initialize node
        rospy.init_node("test_publish")
        rospy.on_shutdown(self.shutdown)

        # Retrieve parameters from launch file
        bootstrap_server = rospy.get_param("~bootstrap_server", "localhost")
        schema_server = rospy.get_param("~schema_server", "http://localhost")
        
        self.use_ssl = rospy.get_param("~use_ssl", False)
        self.use_avro = rospy.get_param("~use_avro", False)

        self.group_id = rospy.get_param("~group_id", None)
        if (self.group_id == "no-group"):
            self.group_id = None

        if (self.use_avro):
            self.avro_subject = rospy.get_param("~avro_subject", "bar-value")

        if (self.use_ssl):
            self.ssl_cafile = rospy.get_param("~ssl_cafile", '../include/certificate.pem')
            self.ssl_keyfile = rospy.get_param("~ssl_keyfile", "../include/kafka.client.keystore.jks")
            self.ssl_password = rospy.get_param("~ssl_password", "password")
            self.ssl_security_protocol = rospy.get_param("~ssl_security_protocol", "SASL_SSL")
            self.ssl_sasl_mechanism = rospy.get_param("~ssl_sasl_mechanism", "PLAIN")
            self.sasl_plain_username = rospy.get_param("~sasl_plain_username", "username")
            self.sasl_plain_password = rospy.get_param("~sasl_plain_password", "password")

        # topics list
        list_of_topics = rospy.get_param("~list_of_topics", [])
        self.list = []
        for item in list_of_topics:
            # TODO: check if some value is missing
            print(item)
            self.list.append(Topic(item["ros_topic"], item["kafka_topic"], item["ros_msg_type"]))

        # Create schema registry connection and serializer
        self.client = CachedSchemaRegistryClient(url=schema_server)
        self.serializer = MessageSerializer(self.client)

        # Create kafka consumer
        # TODO: check possibility of using serializer directly (param value_deserializer from KafkaConsumer)
        for topic in self.list:
            rospy.logwarn("Creating consumer with {} MSGs from KAFKA: {} -> ROS: {}".format(topic.ros_msg_type, topic.kafka_topic, topic.ros_topic))
            if(self.use_ssl):
                topic.consumer = KafkaConsumer(topic.kafka_topic,
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
                topic.consumer = KafkaConsumer(topic.kafka_topic,
                                            bootstrap_servers=bootstrap_server,
                                            auto_offset_reset='latest',
                                            consumer_timeout_ms=5000,
                                            group_id=self.group_id
                                            )

            # Import msg type
            msg_func = ros_loader.get_message_class(topic.ros_msg_type)

            # Subscribe to ROS topic of interest
            topic.publisher = rospy.Publisher(topic.ros_topic, msg_func, queue_size=10)
            rospy.logwarn("Using {} MSGs from KAFKA: {} -> ROS: {}".format(topic.ros_msg_type, topic.kafka_topic, topic.ros_topic))

    def run(self):
        while not rospy.is_shutdown():
            for topic in self.list:
                for msg in topic.consumer:
                    rospy.logwarn("Received MSG from: " + topic.kafka_topic)
                    if (self.use_avro):
                        # Convert Kafka message to Dictionary
                        msg_as_dict = self.serializer.decode_message(msg.value)
                        # Convert Dictionary to ROS Msg
                        ros_msg = message_converter.convert_dictionary_to_ros_message(topic.ros_msg_type, msg_as_dict)
                    else:
                        ros_msg = json_message_converter.convert_json_to_ros_message(topic.ros_msg_type, msg.value)
                    # Publish to ROS topic
                    self.publisher.publish(ros_msg)

    def shutdown(self):
        rospy.loginfo("Shutting down")

if __name__ == "__main__":

    try:
        node = ros_publish()
        #node.run()
    except rospy.ROSInterruptException:
        pass

    rospy.loginfo("Exiting")

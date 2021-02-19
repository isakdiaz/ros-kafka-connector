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

        self.show_received_msg = rospy.get_param("~show_received_msg", False)
        self.show_received_json = rospy.get_param("~show_received_json", False)

        self.use_ssl = rospy.get_param("~use_ssl", False)
        self.use_avro = rospy.get_param("~use_avro", False)
        if not self.use_avro and rospy.has_param("~schema_server"):
            rospy.logwarn("You have set the use_avro parameter to false, but the schema_server parameter exists... Probably that means that you want to use avro. I will use it")
            self.use_avro = True

        if self.use_avro:
            schema_server = rospy.get_param("~schema_server", "http://localhost:8081")

            # Create schema registry connection and serializer
            self.client = CachedSchemaRegistryClient(url=schema_server)
            self.serializer = MessageSerializer(self.client)

        self.group_id = rospy.get_param("~group_id", None)
        if self.group_id == "no-group":
            self.group_id = None

        if self.use_ssl:
            self.ssl_cafile = rospy.get_param("~ssl_cafile", '../include/certificate.pem')
            self.ssl_keyfile = rospy.get_param("~ssl_keyfile", "../include/kafka.client.keystore.jks")
            self.ssl_password = rospy.get_param("~ssl_password", "password")
            self.ssl_security_protocol = rospy.get_param("~ssl_security_protocol", "SASL_SSL")
            self.ssl_sasl_mechanism = rospy.get_param("~ssl_sasl_mechanism", "PLAIN")
            self.sasl_plain_username = rospy.get_param("~sasl_plain_username", "username")
            self.sasl_plain_password = rospy.get_param("~sasl_plain_password", "password")

        # Create kafka consumer
        # TODO: check possibility of using serializer directly (param value_deserializer from KafkaConsumer)
        if self.use_ssl:
            self.consumer = KafkaConsumer(self.kafka_topic,
                                          bootstrap_servers=bootstrap_server,
                                          auto_offset_reset='latest',
                                          security_protocol=self.ssl_security_protocol,
                                          ssl_check_hostname=False,
                                          ssl_cafile=self.ssl_cafile,
                                          ssl_keyfile=self.ssl_keyfile,
                                          sasl_mechanism=self.ssl_sasl_mechanism,
                                          ssl_password=self.ssl_password,
                                          sasl_plain_username=self.sasl_plain_username,
                                          consumer_timeout_ms=1000,
                                          sasl_plain_password=self.sasl_plain_password,
                                          group_id=self.group_id
                                          )
        else:
            self.consumer = KafkaConsumer(self.kafka_topic,
                                          bootstrap_servers=bootstrap_server,
                                          auto_offset_reset='latest',
                                          consumer_timeout_ms=1000,
                                          group_id=self.group_id
                                          )

        self.received_messages_in_total = 0
        self.debug_info_period = rospy.get_param("~debug_info_period", 10)
        if self.debug_info_period != 0:
            self.received_messages_until_last_debug_period = 0
            self.debug_info_timer = rospy.Timer(rospy.Duration(self.debug_info_period), self.debug_callack)

        # Import msg type
        msg_func = ros_loader.get_message_class(self.msg_type)

        # Subscribe to ROS topic of interest
        self.publisher = rospy.Publisher(self.ros_topic, msg_func, queue_size=10)
        rospy.loginfo("Using {} MSGs from {}: {} -> ROS: {}".format(self.msg_type, self.kafka_or_avro_log(), self.kafka_topic, self.ros_topic))

    def debug_callack(self, event):
        received_messages = self.received_messages_in_total - self.received_messages_until_last_debug_period
        self.received_messages_until_last_debug_period = self.received_messages_in_total
        rospy.loginfo('From %s: %s to ROS %s: Received %d in %1.1f seconds (total %d)', self.kafka_or_avro_log(), self.kafka_topic, self.ros_topic, received_messages, self.debug_info_period, self.received_messages_in_total)

    def kafka_or_avro_log(self):
        return ("AVRO", "KAFKA")[self.use_avro]

    def run(self):
        while not rospy.is_shutdown():
            for msg in self.consumer:
                ros_msg = None
                self.received_messages_in_total += 1
                if self.use_avro:
                    try:
                    # Convert Kafka message to Dictionary
                        msg_as_dict = self.serializer.decode_message(msg.value)
                        # Convert Dictionary to ROS Msg
                        ros_msg = message_converter.convert_dictionary_to_ros_message(self.msg_type, msg_as_dict, check_types=False)
                        if self.show_received_json:
                            rospy.loginfo(msg_as_dict)
                    except Exception as e:
                        rospy.logwarn(str(e) + ': time to debug!')
                else:
                    try:
                        ros_msg = json_message_converter.convert_json_to_ros_message(self.msg_type, msg.value)
                        if self.show_received_json:
                            rospy.loginfo(msg.value)
                    except ValueError as e:
                        rospy.logwarn(str(e) + ': probably you are receiving an avro-encoded message, but trying to process it as a plain message')
                    except Exception as e:
                        rospy.logwarn(str(e) + ': time to debug!')

                if ros_msg != None:
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

#!/usr/bin/env python

import rospy
from rospy_message_converter import message_converter
from rospy_message_converter import json_message_converter
import rosbridge_library.internal.ros_loader as ros_loader

from kafka import KafkaProducer
from kafka import KafkaConsumer
from confluent.schemaregistry.client import CachedSchemaRegistryClient
from confluent.schemaregistry.serializers import MessageSerializer

import avro.schema

class kafka_publish():

    def __init__(self):

        # initialize node
        rospy.init_node("kafka_publish")
        rospy.on_shutdown(self.shutdown)

        # Retrieve parameters from launch file
        bootstrap_server = rospy.get_param("~bootstrap_server", "localhost:9092")

        self.ros_topic = rospy.get_param("~ros_topic", "test")
        self.msg_type = rospy.get_param("~msg_type", "std_msgs/String")
        self.kafka_topic = rospy.get_param("~kafka_topic", "bar")

        self.show_sent_msg = rospy.get_param("~show_sent_msg", False)
        self.show_sent_json = rospy.get_param("~show_sent_json", False)
        
        self.use_avro = rospy.get_param("~use_avro", False)
        if not self.use_avro and rospy.has_param("~schema_server"):
            rospy.logwarn("You have set the use_avro parameter to false, but the schema_server parameter exists... Probably that means that you want to use avro. I will use it")
            self.use_avro = True
    
        if self.use_avro:
            self.avro_subject = rospy.get_param("~avro_subject", "")
            self.avro_file = rospy.get_param("~avro_file", "")

            if self.avro_subject == "":
                rospy.logwarn("You have set an empty avro_subject. This is going to fail")

            # Create schema registry connection and serializer
            schema_server = rospy.get_param("~schema_server", "http://localhost:8081")
            self.client = CachedSchemaRegistryClient(url=schema_server)
            self.serializer = MessageSerializer(self.client)
            
            rospy.loginfo("Loading schema for " + self.avro_subject + " from registry server")
            _, self.avro_schema, _ = self.client.get_latest_schema(self.avro_subject)

            if self.avro_schema is None:
                if self.avro_file != "":
                    rospy.loginfo("Loading schema for " + self.avro_subject + " from file " + self.avro_file + " as it does not exist in the server")
                    self.avro_schema = avro.schema.parse(open(self.avro_file).read())
                if self.avro_schema is None:
                    rospy.logerror("Schema for " + self.avro_subject + " cannot be loaded neither from server nor file, this is going to fail")

        self.use_ssl = rospy.get_param("~use_ssl", False)        
        if self.use_ssl:
            self.ssl_cafile = rospy.get_param("~ssl_cafile", 'certificate.pem')
            self.ssl_keyfile = rospy.get_param("~ssl_keyfile", "kafka.client.keystore.jks")
            self.ssl_password = rospy.get_param("~ssl_password", "password")
            self.ssl_security_protocol = rospy.get_param("~ssl_security_protocol", "SASL_SSL")
            self.ssl_sasl_mechanism = rospy.get_param("~ssl_sasl_mechanism", "PLAIN")
            self.sasl_plain_username = rospy.get_param("~sasl_plain_username", "username")
            self.sasl_plain_password = rospy.get_param("~sasl_plain_password", "password")               
        
    
        # Create kafka producer
        # TODO: check possibility of using serializer directly (param value_serializer from KafkaProducer)
        if(self.use_ssl):
            self.producer = KafkaProducer(bootstrap_servers=bootstrap_server,
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
            self.producer = KafkaProducer(bootstrap_servers=bootstrap_server)

        # ROS does not allow a change in msg type once a topic is created. Therefore the msg
        # type must be imported and specified ahead of time.
        msg_func = ros_loader.get_message_class(self.msg_type)
        
        self.received_messages_in_total = 0
        self.debug_info_period = rospy.get_param("~debug_info_period", 10)
        if self.debug_info_period != 0:
            self.received_messages_until_last_debug_period = 0
            self.debug_info_timer = rospy.Timer(rospy.Duration(self.debug_info_period), self.debug_callack)
        
        # Subscribe to the topic with the chosen imported message type
        rospy.Subscriber(self.ros_topic, msg_func, self.callback)
        rospy.loginfo("Using {} MSGs from ROS: {} -> KAFKA: {}".format(self.msg_type, self.ros_topic, self.kafka_topic))
        

    def debug_callack(self, event):
        received_messages = self.received_messages_in_total - self.received_messages_until_last_debug_period
        self.received_messages_until_last_debug_period = self.received_messages_in_total
        rospy.loginfo('Received %d in %1.1f seconds (total %d), from ROS %s, published to Kafka %s', received_messages, self.debug_info_period, self.received_messages_in_total, self.ros_topic, self.kafka_topic)

    def callback(self, msg):
        self.received_messages_in_total += 1

        # Output msg to ROS and send to Kafka server
        if self.show_sent_msg:
            rospy.loginfo("MSG Received: {}".format(msg))
        # Convert from ROS Msg to Dictionary
        msg_as_dict = message_converter.convert_ros_message_to_dictionary(msg)
        # also print as json for debugging purposes
        msg_as_json = json_message_converter.convert_ros_message_to_json(msg)
        if self.show_sent_json:
            rospy.loginfo(msg_as_json)
        # Convert from Dictionary to Kafka message
        # this way is slow, as it has to retrieve last schema
        # msg_as_serial = self.serializer.encode_record_for_topic(self.kafka_topic, msg_as_dict)
        if self.use_avro:
            try:
                msg_as_serial = self.serializer.encode_record_with_schema(self.kafka_topic, self.avro_schema, msg_as_dict)
                self.producer.send(self.kafka_topic, value=msg_as_serial)
            except Exception as e:
                rospy.logwarn(str(e) + ': time to debug!')
        else:
            try:
                self.producer.send(self.kafka_topic, value=msg_as_json)
            except Exception as e:
                rospy.logwarn(str(e) + ': time to debug!')

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

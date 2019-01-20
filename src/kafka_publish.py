#!/usr/bin/env python

import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
import rospy
from rospy_message_converter import json_message_converter
from utils import import_msg_type

class kafka_publish():

    def __init__(self):

        # initialize node
        rospy.init_node("kafka_publish")
        rospy.on_shutdown(self.shutdown)

        # Retrieve parameters from launch file
        bootstrap_server = rospy.get_param("~bootstrap_server", "localhost:9092")
        self.ros_topic = rospy.get_param("~ros_topic", "test")
        self.kafka_topic = rospy.get_param("~kafka_topic", "test")
        self.msg_type = rospy.get_param("~msg_type", "std_msgs/String")


        # Create kafka producer
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_server, value_serializer=lambda m: json.dumps(m).encode('ascii'))

        # ROS does not allow a change in msg type once a topic is created. Therefore the msg
        # type must be imported and specified ahead of time.
        msg_func = import_msg_type(self.msg_type)

        # Subscribe to the topic with the chosen imported message type
        rospy.Subscriber(self.ros_topic, msg_func, self.callback)
       
        rospy.logwarn("Using {} MSGs from ROS: {} -> KAFKA: {}".format(self.msg_type, self.ros_topic,self.kafka_topic))


    def callback(self, msg):
        # Output msg to ROS and send to Kafka server
        rospy.logwarn("MSG Receved: {}".format(msg)) 
        json_str = json_message_converter.convert_ros_message_to_json(msg)
        self.producer.send(self.kafka_topic, json_str)
        

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

#!/usr/bin/env python

import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
import rospy
from rospy_message_converter import json_message_converter
from utils import import_msg_type

class ros_publish():

    def __init__(self):

        # initialize node
        rospy.init_node("ros_publish")
        rospy.on_shutdown(self.shutdown)

        # Retrieve parameters from launch file
        bootstrap_server = rospy.get_param("~bootstrap_server", "localhost:9092")
        self.ros_topic = rospy.get_param("~ros_topic", "test")
        self.kafka_topic = rospy.get_param("~kafka_topic", "test")
        self.msg_type = rospy.get_param("~msg_type", "std_msgs/String")

        # Create kafka consumer
        self.consumer = KafkaConsumer(self.kafka_topic,
                                    bootstrap_servers=bootstrap_server,
                                    value_deserializer=lambda m: json.loads(m.decode('ascii')),
                                    auto_offset_reset='latest',
                                    consumer_timeout_ms=5000)

        # Import msg type
        msg_func = import_msg_type(self.msg_type)
        
        # Subscribe to ROS topic of interest
        self.publisher = rospy.Publisher(self.ros_topic, msg_func, queue_size=10)
        rospy.logwarn("Using {} MSGs from KAFKA: {} -> ROS: {}".format(self.msg_type, self.kafka_topic,self.ros_topic))
        


    def run(self):
 
        while not rospy.is_shutdown():
            for msg in self.consumer:
                # Convert Kafka message to JSON string
                json_str = json.dumps(msg.value)
                # Convert JSON to ROS message
                ros_msg = json_message_converter.convert_json_to_ros_message(self.msg_type, json_str)
                # Publish to ROS topic
                self.publisher.publish(ros_msg)
                rospy.logwarn("Received MSG: {}".format(json_str))
        
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

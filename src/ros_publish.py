#!/usr/bin/env python

import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
from std_msgs.msg import String

class ros_publish():

    def __init__(self):

        # initialize node
        rospy.init("ros_publish")
        rospy.on_shutdown(self.shutdown)

        # Retrieve parameters from launch file
        bootstrap_server = rospy.get_param("~bootstrap_server", "localhost:9092")
        self.ros_topic = rospy.get_param("~ros_topic", "rosout")
        self.kafka_topic = rospy.get_param("~kafka_topic", "rosout")

        # Create kafka consumer
        self.consumer = KafkaConsumer(self.kafka_topic,
                                    bootstrap_servers=bootstrap_server,
                                    value_deserializer=lambda m: json.loads(m.decode('ascii')),
                                    auto_offset_reset='latest',
                                    consumer_timeout_ms=5000)

        # Subscribe to ROS topic of interest
        self.publisher = rospy.Publisher(ros_topic, String, queue_size=10)

        
    def shutdown(self):
        rospy.loginfo("Shutting down")

    def run(self):
 
        while not rospy.is_shutdown():
            # When the kafka consumer receives a msg, publish it to the ROS topic
            for message in self.consumer1:
                self.publsher.publish(message.value["data"])
        
        rospy.spin()

if __name__ == "__main__":

    try:
        node = ros_publish()
        node.run()
    except rospy.ROSInterruptException:
        pass

    rospy.loginfo("Exiting")

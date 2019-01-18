#!/usr/bin/env python

import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
from std_msgs.msg import String

class kafka_publish():

    def __init__(self):

        # initialize node
        rospy.init("kafka_publish")
        rospy.on_shutdown(self.shutdown)

        # Retrieve parameters from launch file
        bootstrap_server = rospy.get_param("~bootstrap_server", "localhost:9092")
        self.ros_topic = rospy.get_param("~ros_topic", "rosout")
        self.kafka_topic = rospy.get_param("~kafka_topic", "rosout")

        # Create kafka producer
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_server, value_serializer=lambda m: json.dumps(m).encode('ascii'))

        # Subscribe to ROS topic of interest
        rospy.Subscriber(topic, String, self.callback)

    def callback(self, msg):
        # Output msg to ROS and send to Kafka server
        rospy.logdebug("MSG Receved: {}".format(msg)) 
        self.producer.send(self.kafka_topic, {"data": msg})
        
    def shutdown(self):
        rospy.loginfo("Shutting down")

    def run(self):
        rate = rospy.Rate(10)
        while not rospy.is_shutdown():
            rate.sleep()            

if __name__ == "__main__":

    try:
        node = kafka_publish()
        node.run()
    except rospy.ROSInterruptException:
        pass

    rospy.loginfo("Exiting")

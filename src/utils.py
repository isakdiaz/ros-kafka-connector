#!/usr/bin/env python

def import_msg_type(msg_type):

    # Adding a new msg type is as easy as including an import and updating the variable 
    if msg_type == "std_msgs/String":
        from std_msgs.msg import String
        subscriber_msg = String
    elif msg_type == "geometry_msgs/Twist":
        from geometry_msgs.msg import Twist
        subscriber_msg = Twist
    elif msg_type == "sensors_msgs/NavSatFix":
        from sensor_msgs.msg import NavSatFix
        subscriber_msg = NavSatFix
    elif msg_type == "LabeledImg":
        from ros_kafka_connector.msg import LabeledImg
        subscriber_msg = LabeledImg
    elif msg_type == "Sample":
        from ros_kafka_connector.msg import Sample
        subscriber_msg = Sample
    elif msg_type == "Sample":
        from ros_kafka_connector.msg import Sample
        subscriber_msg = Sample
    else:
        raise ValueError("MSG NOT SUPPORTED: Only String/Twist/Image are currently supported. \
                          Please add imports to utils.py for specific msg type.")
    
    return subscriber_msg

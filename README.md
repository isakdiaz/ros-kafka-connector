# ros_kafka_connector ROS Package

This is a ROS package for subscribing or publishing to topics using Kafka. 

Settings:
bootstrap_server: Internal IP or External IP address of VM (default: "localhost:9092")
kafka_topic: name of topic on server (default: "test")
ros_topic: name of ros topic (default: "test")
msg_type: Specify the message type. Currently std_msgs/String, geometry_msgs/Image and sensor_msgs/Image are supported. You can add any message type by adding the import to the utils.py file. (default: "std_msgs/String")


## Publish to ROS topic
roslaunch ros_kafka_connector ros_publish.launch

After updating the launch file with the correct settings for your topic, you can test it by pubishing a JSON to your kafka topic using a kafka publisher. You could also send the message to the kafka server through python with the following script.

#### Install kafka library
pip install kafka-python

#### python script
from kafka import KafkaProducer                                  
producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda x: json.dumps(x).encode('ascii')) 
producer.send('test',{'data':'Hello World!'})          # test is topic name



## Publish to Kafka topic
roslaunch ros_kafka_connector kafka_publish.launch

import time
from datetime import datetime
import uuid

from mqtt_spb_wrapper import MetricDataType, MqttSpbEntityDevice
from mqtt_spb_client import SpbMQTTClient

_DEBUG = True

MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_USER = ""
MQTT_PASSW = ""

domain_name = "TestDomain"
edge_node_name = "TestNode"
device_name = "Device"

def callback_command(cmd):
    print("DEVICE received CMD: %s" %  str(cmd))
    
mqtt = SpbMQTTClient()    
device = MqttSpbEntityDevice(domain_name, edge_node_name, device_name, _DEBUG, mqtt=mqtt)

device.on_command = callback_command


_connected = False
while not _connected:
    print("Trying to connect to broker %s:%d ..." % (MQTT_HOST, MQTT_PORT))
    
    _connected = mqtt.connect(
        host=MQTT_HOST,
        port=MQTT_PORT,
        user=MQTT_USER,
        password=MQTT_PASSW,
    )

    if not _connected:
        print("  Error, could not connect. Trying again in a few seconds ...")
        time.sleep(3)

device.publish_birth()


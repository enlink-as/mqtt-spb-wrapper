#
#    --- Sparkplub B SCADA command send application example ---
#
#   The application will create an spB SCADA application node and send a test command for an specific entity.
#
#   Notes:
#       - To set MQTT broker parameters, use config.yml file
#       - To set spB group and eon ids, use config.yml file.
#
import os
import time
from mqtt_spb_wrapper import *


# APPLICATION default configuration parameters -----------------------------------------------
_DEBUG = True   # Enable debug messages

# Sparkplug B parameters
_config_spb_scada_name = "TEST"

_config_spb_domain_name = "1111"
_config_spb_eon_name ="a0:b3:39:44:22:17"
_config_spb_eond_name = "SHELLY"

# MQTT Configuration
_config_mqtt_host = "a266501f03604b68b0a440d318a01218.s1.eu.hivemq.cloud" 
_config_mqtt_port = 8883
USERNAME = "dev-enlink-node"
PASSWORD = "BnwJQVtR1XnpidN"

_config_mqtt_user = USERNAME
_config_mqtt_pass = PASSWORD

print("--- Sparkplug B example - SCADA send cmd example")

# Global variables ----------------------------------------

# Create the SCADA entity to listen to all spB messages
scada = MqttSpbEntityScada(
    spb_domain_name=_config_spb_domain_name,
    spb_scada_name=_config_spb_scada_name,
    debug_enabled=_DEBUG,
)

# ATTRIBUTES
scada.attributes.set_value("description", "SCADA application simple")

# Connect to the broker.
_connected = False
while not _connected:
    print("Connecting to data broker %s:%d ..." % (_config_mqtt_host, _config_mqtt_port))
    _connected = scada.connect(
        _config_mqtt_host,
        _config_mqtt_port,
        _config_mqtt_user,
        _config_mqtt_pass,
        skip_death=True,    # Ghost application
    )
    if not _connected:
        print("  Error, could not connect. Trying again in a few seconds ...")
        time.sleep(3)

time.sleep(4)

# Wait until it is initialized
while not scada.is_connected():
    time.sleep(0.1)

print("Sending command...")
time.sleep(1)

# Send a command to a EoN Device
scada.send_command(cmd_name="test",
                   cmd_value=True,
                   eon_name=_config_spb_eon_name,
                   eond_name=_config_spb_eond_name)

time.sleep(3)
print("Done!")


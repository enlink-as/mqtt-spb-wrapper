import time
import asyncio
import paho.mqtt.client as mqtt

from .spb_base import SpbEntity, SpbTopic, SpbPayloadParser
from .spb_protobuf import getNodeDeathPayload


class MqttSpbEntity(SpbEntity):

    def __init__(self,
                 spb_domain_name,
                 spb_eon_name,
                 spb_eon_device_name,
                 retain_birth=False,
                 debug_enabled=False,
                 debug_id="MQTT_SPB_ENTITY",
                 entity_is_scada=False,
                 mqtt=None
                 ):

        super().__init__(spb_domain_name=spb_domain_name,
                         spb_eon_name=spb_eon_name,
                         spb_eon_device_name=spb_eon_device_name,
                         debug_enabled=debug_enabled,
                         debug_id=debug_id,
                         mqtt=mqtt
                         )

        # Public members -----------
        self.on_command = None
        
        
        self.on_disconnect = None

        # Private members -----------
        self._spb_namespace = "spBv1.0"     # Default spb namespace
        self._retain_birth = retain_birth
        self._entity_is_scada = entity_is_scada
        self._mqtt = mqtt  # Mqtt client object
  
        self._loopback_topic = ""  # Last publish topic, to avoid loopback message reception
        self.config_received_event = asyncio.Event()
        self.received_config = ""


        
    def publish_data(self, send_all=False, qos=0):
        raise NotImplementedError("Must be implemented in subclasses")

    def publish_death(self):
        raise NotImplementedError("Must be implemented in subclasses")
    
    
    def disconnect(self, skip_death_publish=False):
        raise NotImplementedError("Must be implemented in subclasses")

    def is_connected(self):
        if self._mqtt is None:
            return False
        else:
            return self._mqtt.is_connected()

    def _mqtt_payload_publish(self, topic: str, payload:bytes, qos:int = 0, retrain:bool = False):
        """
            Send byte payload via MQTT client
        Args:
            topic:  MQTT topic
            payload: Payload to be sent
            qos: Quality of service
            retrain: retrain flag for message

        Returns: bool true if successful
        """

        if not self.is_connected():
            return False

        # send payload to broker
        res = self._mqtt.publish(topic, payload, qos, retrain)

        return res.is_published()


    def _mqtt_payload_set_last_will (self, topic: str, payload:bytes, qos:int = 1, retrain:bool = False):
        # Set last will payload
        self._mqtt.will_set(topic, payload, qos, retrain)

    def _mqtt_on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self._logger.info("%s - Connected to MQTT server" % self._entity_domain)
            # Subscribing in on_connect() means that if we lose the connection and
            # reconnect then subscriptions will be renewed.
            if len(self._spb_eon_device_name) == 0:  # EoN
                topic = "%s/%s/NCMD/%s" % (self._spb_namespace,
                                           self._spb_domain_name,
                                           self._spb_eon_name)
            else:
                topic = "%s/%s/DCMD/%s/%s" % (self._spb_namespace,
                                              self._spb_domain_name,
                                              self._spb_eon_name,
                                              self._spb_eon_device_name)
            client.subscribe(topic)
            self._logger.info("%s - Subscribed to MQTT topic: %s" % (self._entity_domain, topic))

            # Subscribe to STATE of SCADA application
            topic = "%s/%s/STATE/+" % (self._spb_namespace,
                                       self._spb_domain_name)
            client.subscribe(topic)

            self._logger.info("%s - Subscribed to MQTT topic: %s" % (self._entity_domain, topic))

        else:
            self._logger.error(" %s - Could not connect to MQTT server !" % self._entity_domain)

        # Execute the callback function if it is not None
        if self.on_connect is not None:
            self.on_connect(rc)

    def _mqtt_on_disconnect(self, client, userdata, rc):
        self._logger.info("%s - Disconnected from MQTT server" % self._entity_domain)

        # Execute the callback function if it is not None
        if self.on_disconnect is not None:
            self.on_disconnect(rc)

    def _mqtt_on_message(self, client, userdata, msg):

        # Check if loopback message
        if self._loopback_topic == msg.topic:
            return

        msg_ts_rx = int(time.time() * 1000)  # Save the current timestamp

        # self._logger.info("%s - Message received  %s" % (self._entity_domain, msg.topic))
        # Parse the topic namespace ------------------------------------------------
        topic = SpbTopic(msg.topic)  # Parse and get the topic object

        # Check that the namespace and group are correct
        # NOTE: Should not be because we are subscribed to a specific topic, but good to check.
        if topic.namespace != self._spb_namespace or topic.domain_name != self._spb_domain_name:
            self._logger.warning( "%s - Incorrect MQTT %s namespace or domain. Message ignored !" %
                                  ( self._spb_namespace, self._entity_domain))
            return

        # Check if it is a STATE message from the SCADA application
        if topic.message_type == "STATE":
            # Execute the callback function if it is not None
            if self.on_message is not None:
                self.on_message(topic, msg.payload.decode("utf-8"))
            return

        # Parse the received ProtoBUF data ------------------------------------------------
        payload = SpbPayloadParser().parse_payload(msg.payload)

        # Add the timestamp when the message was received
        payload['timestamp_rx'] = msg_ts_rx

        # Execute the callback function if it is not None
        if self.on_message is not None:
            self.on_message(topic, payload)

        # Actions depending on the MESSAGE TYPE
        if "CMD" in topic.message_type:

            # Check if a list of commands is provided
            if "metrics" not in payload.keys():
                self._logger.error(
                    "%s - Incorrect MQTT CMD payload, could not find any metrics. CMD message ignored !"
                    % self._entity_domain)
                return

            # Process CMDs - Unknown commands will be ignored
            for item in payload['metrics']:

                # If not in the current list of known commands, removed it
                if item['name'] not in self.commands.get_names():
                    self._logger.warning(
                        "%s - Unrecognized CMD: %s - CMD will be ignored" % (self._entity_domain, item['name']))
                    continue  # Process next command

                # Check if the datatypes match, otherwise ignore the command
                elif not isinstance(item['value'], type(self.commands.get_value(item['name']))):
                    self._logger.warning("%s - Incorrect CMD datatype: %s - CMD will be ignored" % (
                        self._entity_domain, item['name']))
                    continue  # Process next command

                # Update the command value. If it has a callback configured, it will be executed
                self.commands.set_value(name=item["name"], value=item["value"], timestamp=item.get("timestamp", None))

            # Execute the callback function if it is not None, with all commands received
            if self.on_command is not None:
                self.on_command(payload)

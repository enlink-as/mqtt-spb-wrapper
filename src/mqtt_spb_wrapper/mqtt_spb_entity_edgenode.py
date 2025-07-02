import asyncio
from .mqtt_spb_entity import MqttSpbEntity

from .spb_protobuf import getDdataPayload, getValueDataType, getNodeDeathPayload
from .spb_protobuf import addMetric
from .spb_base import SpbPayloadParser

class MqttSpbEntityEdgeNode(MqttSpbEntity):

    def __init__(self, spb_domain_name,
                 spb_eon_name,
                 retain_birth=True,
                 debug_enabled=False,
                 debug_id="MQTT_SPB_EDGENODE",
                 include_spb_rebirth=False,
                 device_name=None,
                 mqtt=None):

        # Initialized the object ( parent class ) with Device_id as None - Configuring it as edge node
        super().__init__(spb_domain_name=spb_domain_name, spb_eon_name=spb_eon_name,
                         retain_birth=retain_birth,
                         debug_enabled=debug_enabled,
                         spb_eon_device_name=device_name,
                         debug_id=debug_id,
                         mqtt=mqtt)
        #mqtt.on_message = self.on_message
        mqtt.on_connect_callback_pool[self] = self.on_connect
        mqtt.on_message_callback_pool[self] = self.on_message

        self.command_topic = "%s/%s/NCMD/%s" % (self._spb_namespace,
                                              self._spb_domain_name,
                                              self._spb_eon_name)

        self.received_config = ""
        
        self.topics = [self.command_topic]
        self.listeners = {}
        if self._mqtt.is_connected():
            self.on_connect(self._mqtt, None, None, 0)

        # Add spB Birth command as per Specifications
        if include_spb_rebirth:
            self.commands.set_value(name="Node Control/Rebirth",
                                    value=False)
                                    
    def add_listener(self, callback):
        for topic in self.topics:
            if topic not in self.listeners:
                self.listeners[topic] = []
            self.listeners[topic].append(callback)
            
    def on_message(self, topic, payload):
        parsed_payload = SpbPayloadParser().parse_payload(payload)
        if topic in self.listeners:
            for callback in self.listeners[topic]:
                callback(topic, parsed_payload)
#self.config_received_event.set()

    def on_connect(self, client, userdata, flags, rc):
        for topic in self.topics:
            client.subscribe(topic)
        
    # Do we implement a DEATH command?
    def publish_birth(self, qos=0):

        if not self.is_connected():  # If not connected

            self._logger.warning("%s - Could not send publish_birth(), not connected to MQTT server"
                                 % self._entity_domain)
            return False
    
        if self.is_empty():  # If no data (Data, attributes, commands )

            self._logger.warning(
                "%s - Could not send publish_birth(), entity doesn't have data ( attributes, data, commands )"
                % self._entity_domain)
            return False

        # Publish BIRTH message
        payload_bytes = self.serialize_payload_birth()
        self._logger.info(SpbPayloadParser().parse_payload(payload_bytes))
        topic = "%s/%s/NBIRTH/%s" % (self._spb_namespace,
                                        self._spb_domain_name,
                                        self._spb_eon_name)

        self._loopback_topic = topic
        
        self._mqtt_payload_publish(topic, payload_bytes, qos, self._retain_birth)

        self._logger.info("%s - Published NBIRTH message" % self._entity_domain)

        self.is_birth_published = True

            
    def publish_command_device(self, spb_eon_device_name, commands):

        if not self.is_connected():  # If not connected

            self._logger.warning(
                "%s - Could not send publish_command_device(), not connected to MQTT server" % self._entity_domain)
            return False

        if not isinstance(commands, dict):  # If no data commands as dictionary
            self._logger.warning(
                "%s - Could not send publish_command_device(), commands not provided or not valid. Please provide a dictionary of command:value" % self._entity_domain)
            return False

        # Get a new payload object, to add metrics
        payload = getDdataPayload()

        # Add the list of commands to the payload metrics
        for k in commands:
            addMetric(payload, k, None, getValueDataType(commands[k]), commands[k])

        # Send payload if there is new data
        topic = "%s/%s/DCMD/%s/%s" % (self._spb_namespace,
                                      self._spb_domain_name,
                                      self._spb_eon_name,
                                      spb_eon_device_name)


        if payload.metrics:
            payload_bytes = bytearray(payload.SerializeToString())
            self._loopback_topic = topic
            self._mqtt_payload_publish(topic, payload_bytes)
            
            self._logger.info("%s - Published COMMAND message to %s" % (self._entity_domain, topic))


            return True

        self._logger.warning("%s - Could not publish COMMAND message to %s" % (self._entity_domain, topic))
        return False

    def publish_data(self, send_all=False, qos=0):
        """
            Send the new updated data to the MQTT broker as a Sparkplug B DATA message.

        :param send_all: boolean    True: Send all data fields, False: send only updated field values
        :param qos                  QoS level
        :return:                    result
        """
        if not self.is_connected():  # If not connected

            self._logger.warning(
                "%s - Could not send publish_telemetry(), not connected to MQTT server" % self._entity_domain)
            return False

        if self.is_empty():  # If no data (Data, attributes, commands )
            self._logger.warning(
                "%s - Could not send publish_telemetry(), entity doesn't have data ( attributes, data, commands )"
                % self._entity_domain)
            return False
        # Send payload if there is new data, or we need to send all
        if send_all or self.data.is_updated():
            payload_bytes = self.serialize_payload_data(send_all)  # Get the data payload


            topic = "%s/%s/NDATA/%s" % (self._spb_namespace,
                                        self._spb_domain_name,
                                        self._spb_eon_name)

            self._loopback_topic = topic
            self._mqtt_payload_publish(topic, payload_bytes, qos)

            self._logger.debug("%s - Published DATA message %s" % (self._entity_domain, topic))
            return True

        self._logger.warning("%s - Could not publish DATA message, may be data no new data values?"
                             % self._entity_domain)
        return False

    def disconnect(self, skip_death_publish=False):

        self._logger.info("%s - Disconnecting from MQTT server" % self._entity_domain)

        if self._mqtt is not None:
            
            # Send the DEATH message -
            # If you do a graceful disconnect, the last will is not published automatically by the MQTT Broker.
            if not skip_death_publish:
                payload = getNodeDeathPayload()
                payload_bytes = bytearray(payload.SerializeToString())
                topic = "%s/%s/NDEATH/%s" % (self._spb_namespace,
                                             self._spb_domain_name,
                                             self._spb_eon_name)
                                
                self._mqtt_payload_publish(topic, payload_bytes)  # Set message

        self._mqtt.disconnect()

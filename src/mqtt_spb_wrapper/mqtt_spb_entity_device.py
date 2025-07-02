from .mqtt_spb_entity import MqttSpbEntity
from .spb_protobuf import getNodeDeathPayload
from .spb_base import SpbEntity, SpbTopic, SpbPayloadParser


class MqttSpbEntityDevice(MqttSpbEntity):

    def __init__(self,
                 spb_domain_name,
                 spb_eon_name,
                 spb_eon_device_name,
                 retain_birth=True,
                 debug_enabled=False,
                 mqtt=None
                 ):


        # Initialized the object ( parent class ) with Device_id - Configuring it as edge device
        super().__init__(spb_domain_name=spb_domain_name,
                         spb_eon_name=spb_eon_name,
                         spb_eon_device_name=spb_eon_device_name,
                         retain_birth=retain_birth,
                         debug_enabled=debug_enabled, debug_id="MQTT_SPB_DEVICE",
                         mqtt=mqtt
                         )
        
        mqtt.on_connect_callback_pool[self] = self.on_connect
        mqtt.on_message_callback_pool[self] = self.on_message

        command_topic = "%s/%s/DCMD/%s/%s" % (self._spb_namespace,
                                              self._spb_domain_name,
                                              self._spb_eon_name,
                                              self._spb_eon_device_name)
        state_topic =  "%s/%s/STATE/%s/%s" % (self._spb_namespace,
                                              self._spb_domain_name,
                                              self._spb_eon_name,
                                              self._spb_eon_device_name)

        self.topics = [command_topic, state_topic]
        self.listeners = {}
        if self._mqtt.is_connected():
            self.on_connect(self._mqtt, None, None, 0)
        
    def add_listener(self, callback):
        for topic in self.topics:
            if topic not in self.listeners:
                self.listeners[topic] = []
            self.listeners[topic].append(callback)
        
    def on_connect(self, client, userdata, flags, rc):
        for topic in self.topics:
            client.subscribe(topic)
      
    def on_message(self, topic, payload ):

        parsed_payload = SpbPayloadParser().parse_payload(payload)

        if topic in self.listeners:
            for callback in self.listeners[topic]:
                callback(topic, parsed_payload)
        
    def publish_birth(self, qos=0) -> bool:
        if not self.is_connected:
            self._logger.warning("%s - Could not send publish_birth(), not connected to MQTT server"
                                 % self._entity_domain)
            return False

        payload_bytes = self.serialize_payload_birth()
        topic = "%s/%s/DBIRTH/%s/%s" % (self._spb_namespace,
                                        self._spb_domain_name,
                                        self._spb_eon_name,
                                        self._spb_eon_device_name)
        
        self._loopback_topic = topic
        self._mqtt_payload_publish(topic, payload_bytes, qos, self._retain_birth)
        self._logger.info("%s - Published BIRTH message" % self._entity_domain)
        
        self.is_birth_published = True
        return True


    def publish_data(self, send_all=False, qos=2):
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
            
            topic = "%s/%s/DDATA/%s/%s" % (self._spb_namespace,
                                           self._spb_domain_name,
                                           self._spb_eon_name,
                                           self._spb_eon_device_name)
          
            self._loopback_topic = topic
            self._mqtt_payload_publish(topic, payload_bytes, qos)

            self._logger.debug("%s - Published DDATA message %s" % (self._entity_domain, topic))

            return True

        self._logger.warning("%s - Could not publish DDATA message, may be data no new data values?"
                             % self._entity_domain)
        return False

    def disconnect(self, skip_death_publish=False):

        self._logger.info("%s - Disconnecting from MQTT server" % self._entity_domain)

        if self._mqtt is not None:
            
            # Send the DEATH message -
            # If you do a graceful disconnect, the last will is not published automatically by the MQTT Broker.
            if not skip_death_publish:
                self.publish_death()
        self._mqtt.disconnect()

    def publish_death(self):
    
        payload = getNodeDeathPayload()
        payload_bytes = bytearray(payload.SerializeToString())
        topic = "%s/%s/DDEATH/%s/%s" % (self._spb_namespace,
                                        self._spb_domain_name,
                                        self._spb_eon_name,
                                        self._spb_eon_device_name)
        self._mqtt_payload_publish(topic, payload_bytes)  # Set message

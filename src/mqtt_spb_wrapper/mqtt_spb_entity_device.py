from .mqtt_spb_entity import MqttSpbEntity
from .spb_protobuf import getNodeDeathPayload

class MqttSpbEntityDevice(MqttSpbEntity):

    def __init__(self,
                 spb_domain_name,
                 spb_eon_name,
                 spb_eon_device_name,
                 retain_birth=False,
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

    def publish_birth(self, qos=0) -> bool:
        print("Attempting to publish birth")
        if not self.is_connected:
            print("Could not publish birth. Not connected to MQTT server")
            self._logger.warning("%s - Could not send publish_birth(), not connected to MQTT server"
                                 % self._entity_domain)
            return False

        payload_bytes = self.serialize_payload_birth()
        topic = "%s/%s/DBIRTH/%s/%s" % (self._spb_namespace,
                                        self._spb_domain_name,
                                        self._spb_eon_name,
                                        self._spb_eon_device_name)
        print(f"Topic {topic}")
        self._loopback_topic = topic
        self._mqtt_payload_publish(topic, payload_bytes, qos, self._retain_birth)
        self._logger.info("%s - Published BIRTH message" % self._entity_domain)
        self.is_birth_published = True
        return True


    def publish_data(self, send_all=False, qos=0):
        if not self.is_connected():  # If not connected
            print("not connected to MQTT server")
            self._logger.warning(
                "%s - Could not send publish_telemetry(), not connected to MQTT server" % self._entity_domain)
            return False

        if self.is_empty():  # If no data (Data, attributes, commands )
            print("No data")
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
            print(topic)
            self._loopback_topic = topic
            self._mqtt_payload_publish(topic, payload_bytes, qos)

            self._logger.debug("%s - Published DATA message %s" % (self._entity_domain, topic))
            print("%s - Published DATA message %s" % (self._entity_domain, topic))
                  
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
                topic = "%s/%s/DDEATH/%s/%s" % (self._spb_namespace,
                                                self._spb_domain_name,
                                                self._spb_eon_name,
                                                self._spb_eon_device_name)
                print(f"DEATH topic: {topic}")
                self._mqtt_payload_publish(topic, payload_bytes)  # Set message
        self._mqtt.disconnect()

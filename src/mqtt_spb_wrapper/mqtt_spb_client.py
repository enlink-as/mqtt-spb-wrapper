import time
import paho.mqtt.client as mqtt
import logging

class SpbMQTTClient:

    def __init__(self):
        
        self._mqtt = mqtt.Client()
        self._mqtt.on_disconnect = None #self._mqtt_on_disconnect
        self._mqtt.on_message = self.on_message # self.on_message
        self._mqtt.on_connect = self.on_connect
        self.on_connect_callback_pool = {}
        self.on_message_callback_pool = {}
        
    def on_message(self, client, userdata, message):
        topic = message.topic
        payload = message.payload
        for device, callback in self.on_message_callback_pool.items():
            if topic in device.topics:
                callback(topic, payload)

    def on_connect(self, client, userdata, flags, rc):
        for device_id, callback in self.on_connect_callback_pool.items():
            callback(client)

    def subscribe(self, topic):
        self._mqtt.subscribe(topic)
    
    def connect(self,
        host='localhost',
        port=1883,
        user="",
        password="",
        use_tls=False,
        tls_ca_path="",
        tls_cert_path="",
        tls_key_path="",
        tls_insecure=False,
        timeout=5,
        ):

        """
            Connect to the spB MQTT server
        Args:
            host:
            port:
            user:
            password:
            use_tls:
            tls_ca_path:
            tls_cert_path:
            tls_key_path:
            tls_insecure:
            timeout:


        Returns:

        """
        # If we are already connected, then exit
        if self.is_connected():
            return True

        # MQTT Client configuration
       

        
        if user != "":
            self._mqtt.username_pw_set(user, password)

        # If client certificliencates are provided
        if tls_ca_path and tls_cert_path and tls_key_path:
#            self._logger.debug("Setting CA client certificates")

            if tls_insecure:
               # self._logger.debug(
               #      "Setting CA client certificates - IMPORTANT CA insecure mode ( use only for testing )")

                import ssl
                self._mqtt.tls_set(ca_certs=tls_ca_path, certfile=tls_cert_path, keyfile=tls_key_path,
                                   cert_reqs=ssl.CERT_NONE)
                self._mqtt.tls_insecure_set(True)
            else:
#                self._logger.debug("Setting CA client certificates")
                self._mqtt.tls_set(ca_certs=tls_ca_path, certfile=tls_cert_path, keyfile=tls_key_path)

        # If only CA is provided.
        elif tls_ca_path:
#            self._logger.debug("Setting CA certificate")
            self._mqtt.tls_set(ca_certs=tls_ca_path)

        # If TLS is enabled
        else:
            if use_tls:
                self._mqtt.tls_set()  # Enable TLS encryption


        # Entity DEATH message - last will message
        # This belongs somewhere else
        # if not skip_death:
        #     if self._entity_is_scada:  # If it is a type entity SCADA, change the DEATH certificate
        #         topic = "%s/%s/STATE/%s" % (self._spb_namespace,
        #                                        self._spb_domain_name,
        #                                        self._spb_eon_name)
        #         self._mqtt_payload_set_last_will(topic, "OFFLINE".encode("utf-8"))  # Set message

        #     else:  # Normal node
        #         payload = getNodeDeathPayload()
        #         payload_bytes = bytearray(payload.SerializeToString())
        #         if self._spb_eon_device_name is None:  # EoN
        #             topic = "%s/%s/NDEATH/%s" % (self._spb_namespace,
        #                                            self._spb_domain_name,
        #                                            self._spb_eon_name)
        #         else:
        #             topic = "%s/%s/DDEATH/%s/%s" % (self._spb_namespace,
        #                                            self._spb_domain_name,
        #                                            self._spb_eon_name,
        #                                            self._spb_eon_device_name)
        #         self._mqtt_payload_set_last_will(topic, payload_bytes)  # Set message

        # MQTT Connect
        # self._logger.info("%s - Trying to connect MQTT server %s:%d" % (self._entity_domain, host, port))
        try:
            self._mqtt.connect(host, port)
        except Exception as e:
            # self._logger.warning("%s - Could not connect to MQTT server (%s)" % (self._entity_domain, str(e)))
            return False

        self._mqtt.loop_start()  # Start MQTT background task
        time.sleep(0.1)

        # Wait some time to get connected
        _timeout = time.time() + timeout
        while not self.is_connected() and _timeout > time.time():
            time.sleep(0.1)

        # Return if we connected successfully
        return self.is_connected()

    def disconnect(self):
        self._mqtt.loop_stop()
        time.sleep(0.1)
#        self._mqtt.disconnect()
        time.sleep(0.1)


    
    def is_connected(self):
        if self._mqtt is None:
            return False
        else:
            return self._mqtt.is_connected()

    def publish(self, topic, payload, qos, retain):
        return self._mqtt.publish(topic=topic, payload=payload, qos=qos, retain=retain)
    
    def loop_stop(self):
        self._mqtt.loop_stop()

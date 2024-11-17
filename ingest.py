import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import paho.mqtt.client as mqtt
from google.cloud import firestore
from google.cloud.firestore import Client
import firebase_admin
from firebase_admin import credentials, firestore
import os
from dataclasses import dataclass
import signal
import sys
import requests

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Rule evaluation endpoint
RULE_EVALUATION_URL = "https://us-central1-xxxx-xxxx.cloudfunctions.net/evaluate_rules"

@dataclass
class MQTTConfig:
    broker: str
    port: int = 1883
    username: Optional[str] = None
    password: Optional[str] = None
    topics: list[str] = None
    use_tls: bool = False
    client_id: Optional[str] = None

class MQTTFirestoreIngestor:
    def __init__(self, mqtt_config: MQTTConfig, collection_name: str):
        """
        Initialize the MQTT to Firestore ingestor.
        
        Args:
            mqtt_config: MQTT configuration
            collection_name: Firestore collection to store messages in
        """
        self.mqtt_config = mqtt_config
        self.collection_name = collection_name
        self.is_running = False
        
        # Initialize Firestore
        self._init_firestore()
        
        # Initialize MQTT client
        self._init_mqtt()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _init_firestore(self):
        """Initialize Firestore client."""
        try:
            # Try to initialize with default credentials
            firebase_admin.initialize_app()
            self.db = firestore.client()
            logger.info("Initialized Firestore with default credentials")
        except Exception as e:
            # If that fails, look for a service account key file
            cred_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if not cred_path:
                raise ValueError("No Firestore credentials found. Set GOOGLE_APPLICATION_CREDENTIALS environment variable.")
            
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred)
            self.db = firestore.client()
            logger.info("Initialized Firestore with service account credentials")
    
    def _init_mqtt(self):
        """Initialize MQTT client with configured settings."""
        client_id = self.mqtt_config.client_id or f"firestore-ingestor-{datetime.now().timestamp()}"
        self.mqtt_client = mqtt.Client(
            client_id=client_id,
            protocol=mqtt.MQTTv5,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )
        
        # Set callbacks
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_message = self._on_message
        self.mqtt_client.on_disconnect = self._on_disconnect
        
        # Set authentication if provided
        if self.mqtt_config.username and self.mqtt_config.password:
            self.mqtt_client.username_pw_set(
                self.mqtt_config.username,
                self.mqtt_config.password
            )
        
        # Set TLS if required
        if self.mqtt_config.use_tls:
            self.mqtt_client.tls_set()
    
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        """Callback for when MQTT client connects."""
        logger.info(f"Connected to MQTT broker with result code: {reason_code}")
        
        # Subscribe to configured topics
        if self.mqtt_config.topics:
            for topic in self.mqtt_config.topics:
                self.mqtt_client.subscribe(topic)
                logger.info(f"Subscribed to topic: {topic}")

    def _safe_topic_id(self, topic: str) -> str:
        """Convert MQTT topic to a valid Firestore document ID."""
        # Replace invalid characters with underscores
        return topic.replace('/', '_').replace('.', '_')
    
    def _evaluate_rules(self, topic: str, value: float):
        """Send a GET request to evaluate rules for the given topic and value."""
        try:
            params = {'topic': topic, 'value': value}
            response = requests.get(RULE_EVALUATION_URL, params=params)
            response.raise_for_status()
            logger.info(f"Rule evaluation triggered for topic {topic} with value {value}")
            return response.json()
        except Exception as e:
            logger.error(f"Error evaluating rules: {str(e)}", exc_info=True)
            return None
    
    def _on_message(self, client, userdata, message):
        """Callback for when a message is received."""
        try:
            logger.debug(f"Received message on topic {message.topic}")
            
            # Parse payload as JSON
            try:
                payload = json.loads(message.payload)
                
                # Validate payload structure
                if not isinstance(payload, dict) or 'value' not in payload or 'immediate' not in payload:
                    raise ValueError("Invalid payload structure. Expected 'value' and 'immediate' fields.")
                
                value = payload['value']
                immediate = payload['immediate']
                
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON payload: {str(e)}")
                return
            except ValueError as e:
                logger.error(str(e))
                return
            
            # Create document data
            doc_data = {
                'topic': message.topic,
                'value': value,
                'immediate': immediate,
                'qos': message.qos,
                'timestamp': firestore.SERVER_TIMESTAMP,
                'retain': message.retain,
                'last_updated': datetime.now().isoformat()
            }
            
            # Use topic as document ID (sanitized for Firestore)
            doc_id = self._safe_topic_id(message.topic)
            
            # Store in Firestore using set() to overwrite existing document
            doc_ref = self.db.collection(self.collection_name).document(doc_id)
            doc_ref.collection('messages').add(doc_data)
            # add dummy field to doc_ref
            doc_ref.set({'last_updated': datetime.now().isoformat()})
            
            logger.info(f"Updated document for topic {message.topic} in Firestore")
            logger.debug(f"Document data: {doc_data}")
            
            # If immediate is True, trigger rule evaluation
            if immediate:
                self._evaluate_rules(message.topic, value)
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}", exc_info=True)
    
    def _on_disconnect(self, client, userdata, reason_code, properties):
        """Callback for when MQTT client disconnects."""
        logger.warning(f"Disconnected from MQTT broker with reason code: {reason_code}")
        
        if self.is_running:
            logger.info("Attempting to reconnect...")
            self.mqtt_client.reconnect()
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info("Shutdown signal received")
        self.stop()
        sys.exit(0)
    
    def start(self):
        """Start the ingestor."""
        try:
            self.is_running = True
            
            # Connect to MQTT broker
            logger.info(f"Connecting to MQTT broker {self.mqtt_config.broker}:{self.mqtt_config.port}")
            self.mqtt_client.connect(
                self.mqtt_config.broker,
                self.mqtt_config.port
            )
            
            # Start MQTT loop
            self.mqtt_client.loop_forever()
            
        except Exception as e:
            logger.error(f"Error starting ingestor: {str(e)}", exc_info=True)
            self.stop()
            raise
    
    def stop(self):
        """Stop the ingestor."""
        logger.info("Stopping ingestor")
        self.is_running = False
        self.mqtt_client.disconnect()
        self.mqtt_client.loop_stop()

def main():
    # Example configuration
    mqtt_config = MQTTConfig(
        broker="test.mosquitto.org",
        port=1883,
        topics=["sensors_12458/#"],  # Example topics
        use_tls=False
    )
    
    # Create and start ingestor
    ingestor = MQTTFirestoreIngestor(
        mqtt_config=mqtt_config,
        collection_name="mqtt_values"  # Collection for mqtt values
    )
    
    try:
        logger.info("Starting MQTT to Firestore ingestor")
        ingestor.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    finally:
        ingestor.stop()

if __name__ == "__main__":
    main()
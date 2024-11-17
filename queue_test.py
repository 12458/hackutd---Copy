from dataclasses import dataclass
from typing import List, Dict, Any, Optional, TypeVar, Generic
from enum import Enum
import json
import paho.mqtt.client as mqtt
from datetime import datetime
import smtplib
from email.message import EmailMessage
import logging
from abc import ABC, abstractmethod

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

T = TypeVar('T')

class Operation(Enum):
    AND = "AND"
    OR = "OR"
    GREATER_EQUAL = ">="
    GREATER = ">"
    LESS = "<"
    LESS_EQUAL = "<="
    EQUALS = "=="
    NOT_EQUALS = "!="

@dataclass
class Condition(Generic[T]):
    topic: str
    operation: Operation
    value: T
    
    def __post_init__(self):
        # Validate operation type
        if not isinstance(self.operation, Operation):
            raise ValueError(f"Invalid operation type: {self.operation}")

@dataclass
class Rule:
    name: str
    conditions: List[Condition]
    logical_operator: Operation
    action: str
    action_params: Dict[str, Any]
    
    def __post_init__(self):
        if self.logical_operator not in (Operation.AND, Operation.OR):
            raise ValueError(f"Invalid logical operator: {self.logical_operator}")
        if not self.conditions:
            raise ValueError("Rule must have at least one condition")

class Action(ABC):
    @abstractmethod
    def execute(self, params: Dict[str, Any]) -> None:
        pass

class TodoAction(Action):
    def __init__(self):
        self.todo_list = []
    
    def execute(self, params: Dict[str, Any]) -> None:
        task = params.get("task")
        if not task:
            raise ValueError("Task parameter is required")
            
        todo_item = {
            "task": task,
            "priority": params.get("priority", "medium"),
            "created_at": datetime.now().isoformat()
        }
        self.todo_list.append(todo_item)
        logger.info(f"Added to todo list: {task}")

class EmailAction(Action):
    def __init__(self, config: Optional[Dict[str, str]] = None):
        self.config = config or {}
    
    def execute(self, params: Dict[str, Any]) -> None:
        if not all(key in params for key in ["subject", "body", "to_email"]):
            raise ValueError("Missing required email parameters")
            
        if not self.config:
            logger.info(f"Email would be sent: Subject={params['subject']}, To={params['to_email']}")
            return
            
        msg = EmailMessage()
        msg.set_content(params['body'])
        msg['Subject'] = params['subject']
        msg['From'] = self.config['from_email']
        msg['To'] = params['to_email']
        
        try:
            with smtplib.SMTP_SSL(self.config['smtp_server'], self.config['smtp_port']) as server:
                server.login(self.config['username'], self.config['password'])
                server.send_message(msg)
            logger.info(f"Email sent successfully to {params['to_email']}")
        except Exception as e:
            logger.error(f"Failed to send email: {str(e)}")
            raise

class PhoneAction(Action):
    def execute(self, params: Dict[str, Any]) -> None:
        if not all(key in params for key in ["phone_number", "message"]):
            raise ValueError("Missing required phone call parameters")
        logger.info(f"Phone call would be made to {params['phone_number']} with message: {params['message']}")

class RuleEngine:
    def __init__(self):
        self.rules: List[Rule] = []
        self.latest_values: Dict[str, Any] = {}
        self.actions: Dict[str, Action] = {
            "add_to_todo": TodoAction(),
            "send_email": EmailAction(),
            "make_phone_call": PhoneAction()
        }
        
        # MQTT setup
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_message = self.on_message
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_subscribe = self.on_subscribe  # Added subscribe callback
        
        # Operation mapping
        self.ops = {
            Operation.GREATER_EQUAL: lambda x, y: x >= y,
            Operation.GREATER: lambda x, y: x > y,
            Operation.LESS: lambda x, y: x < y,
            Operation.LESS_EQUAL: lambda x, y: x <= y,
            Operation.EQUALS: lambda x, y: x == y,
            Operation.NOT_EQUALS: lambda x, y: x != y
        }
    
    def add_rule(self, rule: Rule) -> None:
        if not isinstance(rule, Rule):
            raise TypeError("Expected Rule object")
        
        self.rules.append(rule)
        if self.client.is_connected():
            for condition in rule.conditions:
                logger.debug(f"Subscribing to topic: {condition.topic}")
                result = self.client.subscribe(condition.topic)
                logger.debug(f"Subscribe result: {result}")
        else:
            logger.warning("MQTT client not connected when adding rule")
    
    def connect_mqtt(self, broker: str, port: int = 1883, username: Optional[str] = None, 
                    password: Optional[str] = None, use_tls: bool = False) -> None:
        if use_tls:
            self.client.tls_set()
        
        if username and password:
            self.client.username_pw_set(username, password)
        
        try:
            logger.debug(f"Attempting to connect to MQTT broker: {broker}:{port}")
            self.client.connect(broker, port)
            self.client.loop_start()
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {str(e)}")
            raise
    
    def on_connect(self, client, userdata, flags, reason_code, properties):
        logger.info(f"Connected with result code {reason_code}")
        # Resubscribe to all topics in case of reconnection
        for rule in self.rules:
            for condition in rule.conditions:
                print(condition.topic)
                logger.debug(f"Resubscribing to topic: {condition.topic}")
                result = self.client.subscribe(condition.topic)
                logger.debug(f"Resubscribe result: {result}")
    
    def on_subscribe(self, client, userdata, mid, reason_codes, properties):
        logger.debug(f"Subscribed with reason codes: {reason_codes}")
    
    def on_disconnect(self, client, userdata, reason_code, properties):
        logger.warning(f"Disconnected with result code {reason_code}")
    
    def on_message(self, client, userdata, msg):
        logger.debug(f"Received message - Topic: {msg.topic}, Payload: {msg.payload}")
        try:
            try:
                value = json.loads(msg.payload)
            except json.JSONDecodeError:
                value = msg.payload.decode()
                # Try to convert to float if it's a numeric string
                try:
                    value = float(value)
                except ValueError:
                    pass
            
            logger.debug(f"Parsed value: {value} (type: {type(value)})")
            self.latest_values[msg.topic] = value
            self.evaluate_rules()
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}", exc_info=True)
    
    def evaluate_condition(self, condition: Condition) -> bool:
        if condition.topic not in self.latest_values:
            logger.debug(f"Topic {condition.topic} not in latest values")
            return False
                    
        value = self.latest_values[condition.topic]
        logger.debug(f"Evaluating condition - Topic: {condition.topic}, "
                    f"Operation: {condition.operation}, "
                    f"Value: {value} (type: {type(value)}), "
                    f"Target: {condition.value} (type: {type(condition.value)})")
        
        try:
            if condition.operation in (Operation.AND, Operation.OR):
                logging.debug("AND/OR operations not supported for individual conditions")
                result = value == condition.value
            else:
                logging.debug(f"Evaluating condition using operation: {condition.operation} ({value} {condition.operation.value} {condition.value})")
                result = self.ops[condition.operation](value, condition.value)
            logger.debug(f"Condition evaluation result: {result}")
            return result
        except Exception as e:
            logger.error(f"Error evaluating condition: {str(e)}", exc_info=True)
            return False
    
    def evaluate_rules(self) -> None:
        logger.debug("Evaluating all rules")
        for rule in self.rules:
            try:
                logger.debug(f"Evaluating rule: {rule.name}")
                results = [self.evaluate_condition(c) for c in rule.conditions]
                logger.debug(f"Rule conditions results: {results}")
                
                rule_triggered = (all(results) if rule.logical_operator == Operation.AND 
                                else any(results))
                
                logger.debug(f"Rule '{rule.name}' triggered: {rule_triggered}")
                
                if rule_triggered:
                    logger.info(f"Executing action for rule '{rule.name}'")
                    self.execute_action(rule.action, rule.action_params)
            except Exception as e:
                logger.error(f"Error evaluating rule '{rule.name}': {str(e)}", exc_info=True)

if __name__ == "__main__":
    # Example usage
    engine = RuleEngine()
    
    # Add a rule
    rule = Rule(
        name="High Temperature Alert",
        conditions=[
            Condition(topic="sensor_12458/temperature", operation=Operation.GREATER_EQUAL, value=30.0),
            Condition(topic="sensor_12458/humidity", operation=Operation.GREATER_EQUAL, value=60.0)
        ],
        logical_operator=Operation.AND,
        action="add_to_todo",  # Changed to todo for testing
        action_params={
            "task": "Check temperature and humidity!",
            "priority": "high"
        }
    )
    engine.add_rule(rule)
    
    # Connect to MQTT broker
    try:
        engine.connect_mqtt("test.mosquitto.org", port=1883)  # Removed TLS for testing
        
        # For testing, you can publish messages using mosquitto_pub:
        # mosquitto_pub -h test.mosquitto.org -t sensor/temperature -m "31.5"
        # mosquitto_pub -h test.mosquitto.org -t sensor/humidity -m "65.0"
        
        print("Listening for messages. Use Ctrl+C to stop.")
        while True:
            pass
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        engine.stop()
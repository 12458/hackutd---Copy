import functions_framework
from google.cloud import firestore
from datetime import datetime, timedelta
import logging
from typing import Any, Dict, List, Union
from dataclasses import dataclass
from enum import Enum
import operator
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()

topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id="hackutd-9aec1",
    topic='action_queue',
)

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class Operation(Enum):
    AND = "AND"
    OR = "OR"
    GREATER_THAN = ">"
    GREATER_EQUAL = ">="
    LESS_THAN = "<"
    LESS_EQUAL = "<="
    EQUALS = "=="
    NOT_EQUALS = "!="
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"

@dataclass
class Condition:
    topic: str
    operation: Operation
    value: Any
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Condition':
        return cls(
            topic=data['topic'],
            operation=Operation(data['operation']),
            value=data['value']
        )

@dataclass
class LogicalExpression:
    operator: Operation
    operands: List[Union['LogicalExpression', Condition]]
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LogicalExpression':
        operator = Operation(data['operator'])
        operands = []
        
        for operand in data['operands']:
            if 'operator' in operand:
                # This is a nested logical expression
                operands.append(cls.from_dict(operand))
            else:
                # This is a condition
                operands.append(Condition.from_dict(operand))
        
        return cls(operator=operator, operands=operands)

@dataclass
class Rule:
    id: str
    name: str
    logic: LogicalExpression
    description: str = ""
    enabled: bool = True
    
    @classmethod
    def from_dict(cls, rule_id: str, data: Dict[str, Any]) -> 'Rule':
        return cls(
            id=rule_id,
            name=data['name'],
            logic=LogicalExpression.from_dict(data['logic']),
            description=data.get('description', ''),
            enabled=data.get('enabled', True)
        )

class RulesEngine:
    def __init__(self):
        self.db = firestore.Client()
        
        # Operation handlers
        self.ops = {
            Operation.GREATER_THAN: operator.gt,
            Operation.GREATER_EQUAL: operator.ge,
            Operation.LESS_THAN: operator.lt,
            Operation.LESS_EQUAL: operator.le,
            Operation.EQUALS: operator.eq,
            Operation.NOT_EQUALS: operator.ne,
            Operation.CONTAINS: lambda x, y: y in x,
            Operation.NOT_CONTAINS: lambda x, y: y not in x
        }
    
    def _safe_topic_id(self, topic: str) -> str:
        """Convert MQTT topic to Firestore collection name."""
        return topic.replace('/', '_').replace('.', '_').replace('#', 'hash').replace('+', 'plus')
    
    def get_latest_value(self, topic: str) -> Any:
        """Get the latest value for a topic from Firestore."""
        try:
            collection_name = f"mqtt_values/{self._safe_topic_id(topic)}/messages"
            docs = self.db.collection(collection_name).order_by('timestamp', direction=firestore.Query.DESCENDING).limit(1).get()
            
            if not docs:
                logger.warning(f"No metadata found for topic: {topic}")
                return None
            
            for doc in docs:
                metadata_doc = doc.to_dict()
                last_message = metadata_doc.get('value')
                if not last_message:
                    logger.warning(f"No messages found for topic: {topic}")
                    return None
                
                return last_message
        except Exception as e:
            logger.error(f"Error getting latest value for topic {topic}: {e}")
            return None
    
    def evaluate_condition(self, condition: Condition) -> Dict[str, Any]:
        """Evaluate a single condition and return detailed results."""
        try:
            latest_value = self.get_latest_value(condition.topic)
            
            if latest_value is None:
                return {
                    'result': False,
                    'details': {
                        'topic': condition.topic,
                        'operation': condition.operation.value,
                        'target_value': condition.value,
                        'current_value': None,
                        'error': 'No value available'
                    }
                }
            
            # Handle comparison operators
            op_func = self.ops.get(condition.operation)
            if not op_func:
                return {
                    'result': False,
                    'details': {
                        'topic': condition.topic,
                        'operation': condition.operation.value,
                        'error': f"Unknown operation: {condition.operation}"
                    }
                }
            
            # Try to convert values to same type if they differ
            try:
                if isinstance(latest_value, str) and not isinstance(condition.value, str):
                    latest_value = type(condition.value)(latest_value)
                elif isinstance(condition.value, str) and not isinstance(latest_value, str):
                    latest_value = str(latest_value)
            except (ValueError, TypeError):
                return {
                    'result': False,
                    'details': {
                        'topic': condition.topic,
                        'operation': condition.operation.value,
                        'target_value': condition.value,
                        'current_value': latest_value,
                        'error': 'Type conversion failed'
                    }
                }
            
            # Evaluate the condition
            result = op_func(latest_value, condition.value)
            
            return {
                'result': result,
                'details': {
                    'topic': condition.topic,
                    'operation': condition.operation.value,
                    'target_value': condition.value,
                    'current_value': latest_value
                }
            }
            
        except Exception as e:
            return {
                'result': False,
                'details': {
                    'topic': condition.topic,
                    'operation': condition.operation.value,
                    'error': str(e)
                }
            }
    
    def evaluate_expression(self, expr: LogicalExpression) -> Dict[str, Any]:
        """Recursively evaluate a logical expression."""
        try:
            results = []
            details = []
            
            for operand in expr.operands:
                if isinstance(operand, LogicalExpression):
                    # Recursively evaluate nested expression
                    evaluation = self.evaluate_expression(operand)
                    results.append(evaluation['result'])
                    details.append(evaluation)
                else:
                    # Evaluate condition
                    evaluation = self.evaluate_condition(operand)
                    results.append(evaluation['result'])
                    details.append(evaluation['details'])
            
            # Combine results based on logical operator
            if expr.operator == Operation.AND:
                final_result = all(results)
            elif expr.operator == Operation.OR:
                final_result = any(results)
            else:
                raise ValueError(f"Invalid logical operator: {expr.operator}")
            
            return {
                'result': final_result,
                'operator': expr.operator.value,
                'operands': details
            }
            
        except Exception as e:
            logger.error(f"Error evaluating expression: {str(e)}")
            return {
                'result': False,
                'error': str(e),
                'operator': expr.operator.value
            }
    
    def get_rules(self) -> List[Rule]:
        """Get all enabled rules from Firestore."""
        rules = []
        try:
            rules_ref = self.db.collection('rules').get()
            
            for rule_doc in rules_ref:
                try:
                    rule_data = rule_doc.to_dict()
                    if rule_data.get('enabled', True):
                        rule = Rule.from_dict(rule_doc.id, rule_data)
                        rules.append(rule)
                except Exception as e:
                    logger.error(f"Error parsing rule {rule_doc.id}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error getting rules: {str(e)}")
        
        return rules
    
    def evaluate_rule(self, rule: Rule) -> Dict[str, Any]:
        """Evaluate a single rule and return the results."""
        try:
            evaluation = self.evaluate_expression(rule.logic)
            
            return {
                'rule_id': rule.id,
                'rule_name': rule.name,
                'triggered': evaluation['result'],
                'evaluation_time': datetime.now().isoformat(),
                'logic_evaluation': evaluation
            }
            
        except Exception as e:
            logger.error(f"Error evaluating rule {rule.name}: {str(e)}")
            return {
                'rule_id': rule.id,
                'rule_name': rule.name,
                'triggered': False,
                'error': str(e),
                'evaluation_time': datetime.now().isoformat()
            }
    
    def evaluate_all_rules(self) -> List[Dict[str, Any]]:
        """Evaluate all enabled rules and return results."""
        rules = self.get_rules()
        results = []
        
        for rule in rules:
            result = self.evaluate_rule(rule)
            results.append(result)
            
            if result.get('triggered', False):
                logger.info(f"Rule '{rule.name}' ({rule.id}) triggered!")
                future = publisher.publish(topic_name, rule.id.encode())
                logger.debug(future.result())
                logger.info(f"Details: {result}")
            else:
                logger.debug(f"Rule '{rule.name}' not triggered")
        
        return results

@functions_framework.http
def evaluate_rules(request):
    """Cloud Function entry point."""
    try:
        engine = RulesEngine()
        results = engine.evaluate_all_rules()
        
        return {
            'status': 'success',
            'timestamp': datetime.now().isoformat(),
            'rule_results': results
        }
        
    except Exception as e:
        logger.error(f"Error in rules evaluation: {str(e)}")
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }
    
if __name__ == '__main__':
    # Run the rules evaluation
    evaluate_rules(None)
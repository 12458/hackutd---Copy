import functions_framework
from google.cloud import firestore
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
import logging
from google.cloud import pubsub_v1
import json

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

publisher = pubsub_v1.PublisherClient()

topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id="hackutd-9aec1",
    topic='action_queue',
)

class NodeType(Enum):
    GET_DATA = "get_data"
    COMPARE = "compare"
    AND = "and"
    OR = "or"
    PUBLISH = "publish"
    END = "end"

class Operation(Enum):
    GREATER_THAN = ">"
    GREATER_EQUAL = ">="
    LESS_THAN = "<"
    LESS_EQUAL = "<="
    EQUALS = "=="
    NOT_EQUALS = "!="
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"

@dataclass
class Node:
    id: str
    type: NodeType
    properties: Dict[str, Any]
    next: List[str]
    next_true: Optional[List[str]] = None
    next_false: Optional[List[str]] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Node':
        return cls(
            id=data['id'],
            type=NodeType(data['type']),
            properties=data.get('properties', {}),
            next=data.get('next', []),
            next_true=data.get('next_true', []),
            next_false=data.get('next_false', [])
        )

@dataclass
class NodeResult:
    success: bool
    value: Any
    details: Dict[str, Any]

class NodeBasedRulesEngine:
    def __init__(self):
        self.db = firestore.Client()
        self.execution_context: Dict[str, Any] = {}
        
    def _safe_topic_id(self, topic: str) -> str:
        """Convert MQTT topic to Firestore collection name."""
        return topic.replace('/', '_').replace('.', '_').replace('#', 'hash').replace('+', 'plus')
    
    def get_latest_value(self, topic: str) -> Any:
        """Get the latest value for a topic from Firestore."""
        try:
            collection_name = f"mqtt_values/{self._safe_topic_id(topic)}/messages"
            docs = self.db.collection(collection_name).order_by('timestamp', direction=firestore.Query.DESCENDING).limit(1).get()
            
            for doc in docs:
                metadata_doc = doc.to_dict()
                return metadata_doc.get('value')
                
            return None
        except Exception as e:
            logger.error(f"Error getting latest value for topic {topic}: {e}")
            return None

    def execute_get_data_node(self, node: Node) -> NodeResult:
        """Execute a get_data node."""
        logging.debug(f"Execute Get Data Node: {node.properties}")
        topic = node.properties.get('topic')
        if not topic:
            return NodeResult(False, None, {'error': 'No topic specified'})
        
        value = self.get_latest_value(topic)
        return NodeResult(
            success=value is not None,
            value=value,
            details={'topic': topic, 'value': value}
        )

    def execute_compare_node(self, node: Node) -> NodeResult:
        """Execute a compare node."""

        logging.debug(f"Execute Compare Node: {node.properties}")
        try:
            # Get input1 - either from context or direct value
            input1_ref = node.properties['input1']
            input1 = self.execution_context.get(input1_ref) if input1_ref in self.execution_context else input1_ref
            
            # Get input2 - either from context or direct value
            input2_ref = node.properties['input2']
            input2 = self.execution_context.get(input2_ref) if input2_ref in self.execution_context else input2_ref
            
            operation = Operation(node.properties['operator'])
            
            ops = {
                Operation.GREATER_THAN: lambda x, y: x > y,
                Operation.GREATER_EQUAL: lambda x, y: x >= y,
                Operation.LESS_THAN: lambda x, y: x < y,
                Operation.LESS_EQUAL: lambda x, y: x <= y,
                Operation.EQUALS: lambda x, y: x == y,
                Operation.NOT_EQUALS: lambda x, y: x != y,
                Operation.CONTAINS: lambda x, y: y in x,
                Operation.NOT_CONTAINS: lambda x, y: y not in x
            }
            
            if operation not in ops:
                return NodeResult(False, None, {'error': f'Invalid operation: {operation}'})
            
            result = ops[operation](input1, input2)
            return NodeResult(
                success=True,
                value=result,
                details={
                    'input1': input1,
                    'input2': input2,
                    'operation': operation.value,
                    'result': result
                }
            )
        except Exception as e:
            return NodeResult(False, None, {'error': str(e)})

    def execute_logical_node(self, node: Node) -> NodeResult:
        """Execute an AND or OR node."""
        logging.debug(f"Execute Logical Node: {node.properties}")
        try:
            input_values = [self.execution_context.get(input_id) for input_id in node.properties['inputs']]
            
            if node.type == NodeType.AND:
                result = all(input_values)
            else:  # OR
                result = any(input_values)
                
            return NodeResult(
                success=True,
                value=result,
                details={
                    'inputs': input_values,
                    'operation': node.type.value,
                    'result': result
                }
            )
        except Exception as e:
            return NodeResult(False, None, {'error': str(e)})

    def execute_publish_node(self, node: Node) -> NodeResult:
        """Execute a publish node."""
        logging.debug(f"Publishing message: {node.properties}")
        try:
            action = node.properties.get('action')
            action_data = node.properties.get('action_data', {})

            data = {
                'action': action,
                'action_data': action_data
            }

            attributes = {
                'Content-Type': 'application/json'
            }

            future = publisher.publish(topic_name, json.dumps(data).encode(), **attributes)
            future.result()
            
            return NodeResult(
                success=True,
                value="published",
                details={'action': action, 'action_data': action_data}
            )
        except Exception as e:
            return NodeResult(False, None, {'error': str(e)})

    def execute_node(self, node: Node) -> NodeResult:
        """Execute a single node based on its type."""
        node_executors = {
            NodeType.GET_DATA: self.execute_get_data_node,
            NodeType.COMPARE: self.execute_compare_node,
            NodeType.AND: self.execute_logical_node,
            NodeType.OR: self.execute_logical_node,
            NodeType.PUBLISH: self.execute_publish_node,
            NodeType.END: lambda _: NodeResult(True, None, {})
        }
        
        executor = node_executors.get(node.type)
        if not executor:
            return NodeResult(False, None, {'error': f'Unknown node type: {node.type}'})
        
        result = executor(node)
        self.execution_context[node.id] = result.value
        return result

    def execute_rule(self, rule: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a complete rule by traversing its node tree."""
        try:
            self.execution_context = {}
            nodes = {node['id']: Node.from_dict(node) for node in rule['nodes']}
            current_node = nodes[rule['start_node']]
            results = []
            
            while current_node.type != NodeType.END:
                result = self.execute_node(current_node)
                results.append({
                    'node_id': current_node.id,
                    'type': current_node.type.value,
                    'result': result.value,
                    'details': result.details
                })
                
                if not result.success:
                    break
                
                # Determine next node based on result
                next_nodes = []
                if current_node.type == NodeType.COMPARE:
                    next_nodes = current_node.next_true if result.value else current_node.next_false
                else:
                    next_nodes = current_node.next
                
                if not next_nodes:
                    break
                    
                current_node = nodes[next_nodes[0]]
            
            return {
                'rule_id': rule.get('id'),
                'rule_name': rule.get('name'),
                'triggered': any(r['result'] == "published" for r in results if r['type'] == 'publish'),
                'evaluation_time': datetime.now().isoformat(),
                'node_results': results
            }
            
        except Exception as e:
            logger.error(f"Error executing rule: {str(e)}")
            return {
                'rule_id': rule.get('id'),
                'rule_name': rule.get('name'),
                'triggered': False,
                'error': str(e),
                'evaluation_time': datetime.now().isoformat()
            }

    def get_rules(self) -> List[Dict[str, Any]]:
        """Get all enabled rules from Firestore."""
        rules = []
        try:
            rules_ref = self.db.collection('rules').get()
            
            for rule_doc in rules_ref:
                try:
                    rule_data = rule_doc.to_dict()
                    # Check if rule interval has passed
                    last_run = datetime.fromisoformat(rule_data.get('last_run'))
                    interval = rule_data.get('interval', 3600)
                    if rule_data.get('enabled', True) and (datetime.now() - last_run).total_seconds() >= interval:
                        logger.debug(f"Processing rule {rule_doc.id}")
                        rule_data['id'] = rule_doc.id
                        rules.append(rule_data)
                        # Update last run time
                        rule_doc.reference.update({'last_run': datetime.now().isoformat()})
                    else:
                        logger.debug(f"Skipping rule {rule_doc.id} due to interval {interval} < {(datetime.now() - last_run).total_seconds()} or enabled: {rule_data.get('enabled')}")
                except Exception as e:
                    logger.error(f"Error parsing rule {rule_doc.id}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error getting rules: {str(e)}")
        
        return rules

    def evaluate_all_rules(self) -> List[Dict[str, Any]]:
        """Evaluate all enabled rules and return results."""
        rules = self.get_rules()
        results = []
        
        for rule in rules:
            result = self.execute_rule(rule)
            results.append(result)
            
            if result.get('triggered', True):
                logger.info(f"Rule '{rule.get('name')}' ({rule.get('id')}) triggered!")
                logger.info(f"Details: {result}")
            else:
                logger.debug(f"Rule '{rule.get('name')}' not triggered")
        
        return results

@functions_framework.http
def evaluate_rules(request):
    """Cloud Function entry point."""
    try:
        engine = NodeBasedRulesEngine()
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
    evaluate_rules(None)
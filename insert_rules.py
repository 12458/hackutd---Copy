import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from typing import List, Dict, Any
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def initialize_firestore():
    """Initialize Firestore client."""
    try:
        firebase_admin.initialize_app()
    except ValueError:
        # App already initialized
        pass
    return firestore.client()


def insert_rules(rules: List[Dict[str, Any]]) -> None:
    """Insert rules into Firestore."""
    db = initialize_firestore()
    rules_ref = db.collection("rules")

    for rule in rules:
        try:
            # Use rule name as document ID (sanitized)
            doc_id = rule["id"].lower().replace(" ", "_")
            rules_ref.document(doc_id).set(rule)
            logger.info(f"Inserted rule: {rule['id']}")
        except Exception as e:
            logger.error(
                f"Error inserting rule {rule.get('name', 'unknown')}: {str(e)}"
            )


def main():
    # Example rules with increasing complexity
    rules = [
        # Simple single condition rule
        {
            "id": "temperature_difference_alert",
            "name": "Temperature Difference Alert",
            "enabled": True,
            "description": "Alert when temperature sensor 1 is higher than sensor 2",
            "start_node": "get_temp1",
            "nodes": [
                {
                    "id": "get_temp1",
                    "type": "get_data",
                    "properties": {"topic": "sensors_12458_temperature"},
                    "next": ["get_temp2"],
                },
                {
                    "id": "get_temp2",
                    "type": "get_data",
                    "properties": {"topic": "sensors_12459_temperature"},
                    "next": ["compare_temps"],
                },
                {
                    "id": "compare_temps",
                    "type": "compare",
                    "properties": {
                        "input1": "get_temp1",
                        "input2": "get_temp2",
                        "operator": ">",
                    },
                    "next_true": ["publish_alert"],
                    "next_false": ["end"],
                },
                {
                    "id": "publish_alert",
                    "type": "publish",
                    "properties": {
                        "rule_id": "temperature_difference_alert",
                        "action": "email",
                        "action_data": {
                            "body": "Temperature sensor 1 is higher than sensor 2",
                            "to": "alerts@example.com",
                        },
                    },
                    "next": ["end"],
                },
                {"id": "end", "type": "end"},
            ],
        }
        
    ]

    # Insert rules into Firestore
    insert_rules(rules)


if __name__ == "__main__":
    main()

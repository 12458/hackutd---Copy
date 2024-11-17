# dump firestore collection to json
import json
import firebase_admin
from google.cloud import firestore

def dump_collection_to_json(collection_name: str, output_file: str):
    """
    Dump a Firestore collection to a JSON file.
    
    Args:
        collection_name: Name of the Firestore collection
        output_file: Output JSON file
    """
    db = firestore.Client()
    docs = db.collection(collection_name).get()
    print(f'Dumping {len(docs)} documents to {output_file}...')
    data = {doc.id: doc.to_dict() for doc in docs}
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=4, default=str)

def query_collection(collection_name: str, query: list):
    """
    Query a Firestore collection and return the results.
    
    Args:
        collection_name: Name of the Firestore collection
        query: List of query conditions
    """
    db = firestore.Client()
    docs = db.collection(collection_name).where(*query).get()
    return {doc.id: doc.to_dict() for doc in docs}

def query_latest_document(collection_name: str):
    """
    Query the latest document in a Firestore collection.
    
    Args:
        collection_name: Name of the Firestore collection
    """
    db = firestore.Client()
    docs = db.collection(collection_name).order_by('timestamp', direction=firestore.Query.DESCENDING).limit(1).get()
    return {doc.id: doc.to_dict() for doc in docs}

if __name__ == '__main__':
    firebase_admin.initialize_app()
    dump_collection_to_json('rules', 'output.json')
    #print(query_latest_document('mqtt_values/sensors_12458_temperature/messages'))
# This script will dump the contents of the 'my_collection' Firestore collection to a file named 'output.json'.
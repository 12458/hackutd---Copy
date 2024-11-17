import firebase_admin
from firebase_admin import credentials, firestore
from functools import lru_cache

class FirebaseConfig:
    def __init__(self):
        # Initialize Firebase Admin SDK
        firebase_admin.initialize_app()
        self.db = firestore.client()

@lru_cache()
def get_firebase_config():
    return FirebaseConfig()
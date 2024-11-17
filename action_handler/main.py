import functions_framework
from google.cloud import firestore
import smtplib
from email.mime.text import MIMEText
import logging
import json

# Initialize Firestore client
db = firestore.Client()

def send_email(to_address, body):
    """Helper function to send emails"""
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    smtp_username = "xxx@gmail.com"
    smtp_password = "xxx"
    sender_email = "xxx@gmail.com"

    msg = MIMEText(body)
    msg['Subject'] = 'Automated Notification'
    msg['From'] = sender_email
    msg['To'] = to_address

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.send_message(msg)

def add_todo(body):
    """Helper function to add a todo item to Firestore"""
    todos_ref = db.collection('todos')
    todos_ref.add({
        'body': body,
        'created_at': firestore.SERVER_TIMESTAMP,
        'completed': False
    })

def process_rule(action, action_data):
    """Process the rule based on its action type"""
    if action == 'email':
        if not all(k in action_data for k in ('to', 'body')):
            raise ValueError("Email action missing required fields")
        send_email(action_data['to'], action_data['body'])
        
    elif action == 'add_todo':
        if 'body' not in action_data:
            raise ValueError("Todo action missing required body field")
        add_todo(action_data['body'])
        
    else:
        raise ValueError(f"Unknown action type: {action}")

@functions_framework.http
def action_handler(request):
    """Main function to handle incoming HTTP requests"""
    try:
        # Get the raw body and deserialize JSON
        data = request.get_json()

        action = data.get('action')
        action_data = data.get('action_data', {})
        
        if not action or not action_data:
            return 'Invalid Format', 400

        # Process the rule
        process_rule(action, action_data)
        
        return f'Successfully processed {action} {action_data}', 200
        
    except Exception as e:
        logging.error(f"Error processing request: {str(e)}")
        return str(e), 500

# gcloud functions deploy action_handler --runtime python312 --trigger-http --allow-unauthenticated --gen2
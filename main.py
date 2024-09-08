import json
import logging
import os
from flask import Flask, request, jsonify
from google.auth import default
from azure.identity import ClientSecretCredential
from get_files import get_files
from send_files import send_files

# Load configuration from config.json
with open('config.json') as config_file:
    config = json.load(config_file)

app = Flask(__name__)

@app.route('/external_file_transfer', methods=['POST'])
def external_file_transfer():
    logging.info('HTTP request received.')

    action = request.json.get('action')
    if not action:
        return jsonify({"error": "Please provide an action parameter."}), 400

    use = config.get('use', 'azure')
    
    if use == 'azure':
        try:
            # Azure AD credentials from environment variables
            client_id = os.getenv("AZURE_CLIENT_ID")
            client_secret = os.getenv("AZURE_CLIENT_SECRET")
            tenant_id = os.getenv("AZURE_TENANT_ID")
            credential = ClientSecretCredential(tenant_id, client_id, client_secret)

            if action == 'get':
                result = get_files(config, credential)
                return jsonify({"result": result})
            
            elif action == 'send':
                result = send_files(config, credential)
                return jsonify({"result": result})
            
            else:
                return jsonify({"error": "Invalid action. Use 'get' or 'send'."}), 400
        
        except Exception as e:
            logging.error(f"An error occurred: {e}", exc_info=True)
            return jsonify({"error": f"An error occurred: {e}"}), 500

    elif use == 'gcp':
        try:
            # GCP credentials from default environment
            credentials, project = default()

            if action == 'get':
                result = get_files(config, credentials)
                return jsonify({"result": result})
            
            elif action == 'send':
                result = send_files(config, credentials)
                return jsonify({"result": result})
            
            else:
                return jsonify({"error": "Invalid action. Use 'get' or 'send'."}), 400
        
        except Exception as e:
            logging.error(f"An error occurred: {e}", exc_info=True)
            return jsonify({"error": f"An error occurred: {e}"}), 500

    return jsonify({"error": "Invalid configuration. Use 'azure' or 'gcp'."}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))

import os
import json

#first we make a folder (in case it doesn't exist) called .secrets
if not os.path.isdir(".secrets/"):
    os.mkdir(".secrets/")

def create_credentials():
    # Data to be written
    dictionary = {
    "type": os.environ["service_account"],
    "project_id": os.environ["project_id"],
    "private_key_id": os.environ["private_key_id"],
    "private_key": os.environ["private_key"],
    "client_email": os.environ["client_email"],
    "client_id": os.environ["client_id"],
    "auth_uri": os.environ["auth_uri"],
    "token_uri": os.environ["token_uri"],
    "auth_provider_x509_cert_url": os.environ["auth_provider_x509_cert_url"],
    "client_x509_cert_url": os.environ["client_x509_cert_url"],
    "universe_domain": os.environ["universe_domain"]
    }
    
    # Serializing json
    json_object = json.dumps(dictionary)
    
    # Writing to sample.json
    with open("gcp_credentials.json", "w") as outfile:
        outfile.write(json_object)
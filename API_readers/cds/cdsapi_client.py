import cdsapi
from dotenv import load_dotenv
import os

def cdsapi_client_init():
    # Load environment variables from the .env file
    load_dotenv('.env')
    cds_url = os.getenv("CDSAPI_URL")
    cds_key = os.getenv("CDSAPI_KEY")

    # Initialise the client
    return cdsapi.Client(url=cds_url, key=cds_key)
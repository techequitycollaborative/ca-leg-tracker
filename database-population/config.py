import os
from dotenv import load_dotenv

# Load variables from .env - NEVER committed to Git
# On DigitalOcean this should do nothing
load_dotenv()

def get_config(key, default=None):
    """
    Retrieves configuration value from the environment
    """
    value = os.getenv(key, default)
    if not value:
        raise EnvironmentError(f"Missing required environment. variable: {key}")
    return value 
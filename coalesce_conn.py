#!/usr/bin/env python3
"""
Coalesce API Configuration Loader
Loads API configuration from environment variables
"""

import os
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config_from_env():
    """
    Load Coalesce API configuration from environment variables
    
    Returns:
        dict: Configuration dictionary with base_url and access_token
    """
    # Load environment variables from .env file
    load_dotenv()
    
    logger.info("Logging configured: level=INFO")
    
    # Get required environment variables
    base_url = os.getenv('COALESCE_BASE_URL')
    access_token = os.getenv('COALESCE_ACCESS_TOKEN')
    
    if not base_url:
        logger.error("COALESCE_BASE_URL environment variable not found")
        return None
    
    if not access_token:
        logger.error("COALESCE_ACCESS_TOKEN environment variable not found")
        return None
    
    # Clean up base URL (remove trailing slash)
    base_url = base_url.rstrip('/')
    
    config = {
        'base_url': base_url,
        'access_token': access_token
    }
    
    logger.info("Configuration loaded from environment variables")
    
    return config

def validate_config(config):
    """
    Validate the configuration
    
    Args:
        config (dict): Configuration to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    if not config:
        return False
    
    required_keys = ['base_url', 'access_token']
    
    for key in required_keys:
        if key not in config or not config[key]:
            logger.error(f"Missing required configuration key: {key}")
            return False
    
    # Validate base URL format
    if not config['base_url'].startswith(('http://', 'https://')):
        logger.error("base_url must start with http:// or https://")
        return False
    
    # Validate token format (basic check)
    if len(config['access_token']) < 10:
        logger.error("access_token appears to be too short")
        return False
    
    return True

if __name__ == "__main__":
    """Test configuration loading when run directly"""
    print("🔍 Testing Coalesce API Configuration")
    print("=" * 40)
    
    config = load_config_from_env()
    
    if config:
        if validate_config(config):
            print("✅ Configuration loaded successfully!")
            print(f"🔗 Base URL: {config['base_url']}")
            print(f"🔑 Token: {config['access_token'][:10]}..." if len(config['access_token']) > 10 else "🔑 Token: [REDACTED]")
        else:
            print("❌ Configuration validation failed")
    else:
        print("❌ Failed to load configuration")
        print("\n💡 Make sure you have a .env file with:")
        print("COALESCE_BASE_URL=https://petiq.app.coalescesoftware.io")
        print("COALESCE_ACCESS_TOKEN=your_token_here")
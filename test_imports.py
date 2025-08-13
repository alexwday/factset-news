#!/usr/bin/env python3
"""
Test script to verify all imports and module functionality
This will help ensure everything works before actual API calls
"""

import sys
import os

print("=" * 60)
print("TESTING STREET ACCOUNT NEWS MODULE IMPORTS")
print("=" * 60)

# Test 1: Core Python imports
print("\n1. Testing core Python imports...")
try:
    import os
    import tempfile
    import logging
    import json
    import time
    from datetime import datetime, timedelta
    from urllib.parse import quote
    from typing import Dict, Any, Optional, List, Tuple
    import io
    import re
    from pathlib import Path
    print("✓ Core Python imports successful")
except ImportError as e:
    print(f"✗ Core Python import failed: {e}")
    sys.exit(1)

# Test 2: Third-party libraries
print("\n2. Testing third-party library imports...")
try:
    import yaml
    print(f"  ✓ yaml version: {yaml.__version__ if hasattr(yaml, '__version__') else 'unknown'}")
    
    import requests
    print(f"  ✓ requests version: {requests.__version__}")
    
    import pandas as pd
    print(f"  ✓ pandas version: {pd.__version__}")
    
    import openpyxl
    print(f"  ✓ openpyxl version: {openpyxl.__version__}")
    
    from dotenv import load_dotenv
    print("  ✓ python-dotenv imported")
    
    from smb.SMBConnection import SMBConnection
    print("  ✓ pysmb imported")
    
except ImportError as e:
    print(f"✗ Third-party library import failed: {e}")
    sys.exit(1)

# Test 3: FactSet SDK imports
print("\n3. Testing FactSet SDK imports...")
try:
    import fds.sdk.StreetAccountNews
    print("  ✓ fds.sdk.StreetAccountNews imported")
    
    from fds.sdk.StreetAccountNews.api import headlines_api, filters_api, views_api
    print("  ✓ API modules imported (headlines_api, filters_api, views_api)")
    
    from fds.sdk.StreetAccountNews.models import (
        HeadlinesRequest,
        HeadlinesRequestData,
        HeadlinesRequestTickersObject,
        HeadlinesRequestDataSearchTime,
        HeadlinesRequestMeta,
        HeadlinesRequestMetaPagination
    )
    print("  ✓ Model classes imported")
    
except ImportError as e:
    print(f"✗ FactSet SDK import failed: {e}")
    print("\nMake sure you've activated the virtual environment:")
    print("  source venv/bin/activate")
    sys.exit(1)

# Test 4: Configuration testing
print("\n4. Testing configuration capabilities...")
try:
    # Test creating a Configuration object
    test_config = fds.sdk.StreetAccountNews.Configuration(
        username="test_user",
        password="test_pass"
    )
    print("  ✓ Configuration object created")
    
    # Test available configuration attributes
    config_attrs = [attr for attr in dir(test_config) if not attr.startswith('_')]
    print(f"  ✓ Configuration has {len(config_attrs)} attributes")
    
    # Check for important attributes
    important_attrs = ['username', 'password', 'proxy', 'ssl_ca_cert', 'host']
    for attr in important_attrs:
        if hasattr(test_config, attr):
            print(f"    - {attr}: ✓")
        else:
            print(f"    - {attr}: ✗ (missing)")
    
except Exception as e:
    print(f"✗ Configuration test failed: {e}")

# Test 5: Model creation testing
print("\n5. Testing model creation...")
try:
    # Test creating a ticker object
    test_ticker = HeadlinesRequestTickersObject(
        value="RY-CA",
        type="Equity"
    )
    print("  ✓ HeadlinesRequestTickersObject created")
    
    # Test creating search time
    test_search_time = HeadlinesRequestDataSearchTime(
        start=datetime.now() - timedelta(days=7),
        end=datetime.now()
    )
    print("  ✓ HeadlinesRequestDataSearchTime created")
    
    # Test creating pagination
    test_pagination = HeadlinesRequestMetaPagination(
        limit=50,
        offset=0
    )
    print("  ✓ HeadlinesRequestMetaPagination created")
    
    # Test creating full request with CORRECT attributes
    test_request = HeadlinesRequest(
        data=HeadlinesRequestData(
            tickers=[test_ticker],
            search_time=test_search_time
        ),
        meta=HeadlinesRequestMeta(
            pagination=test_pagination,
            attributes=["headlines", "storyTime", "id", "primarySymbols", "symbols", "storyBody"]
        )
    )
    print("  ✓ Full HeadlinesRequest created with correct attributes")
    
except Exception as e:
    print(f"✗ Model creation test failed: {e}")

# Test 6: Local file operations
print("\n6. Testing local file operations...")
try:
    # Test YAML loading
    if os.path.exists("config.yaml"):
        with open("config.yaml", 'r') as f:
            test_config = yaml.safe_load(f)
        print(f"  ✓ config.yaml loaded successfully")
        print(f"    - Found {len(test_config.get('monitored_institutions', {}))} institutions")
    else:
        print("  ⚠ config.yaml not found (expected if not created yet)")
    
    # Test .env loading
    if os.path.exists(".env"):
        load_dotenv()
        print("  ✓ .env file loaded")
        # Check for some env variables (won't have values in template)
        env_vars = ["API_USERNAME", "API_PASSWORD", "PROXY_USER"]
        for var in env_vars:
            value = os.getenv(var)
            if value:
                print(f"    - {var}: set (value hidden)")
            else:
                print(f"    - {var}: not set")
    else:
        print("  ⚠ .env file not found (create from .env.template)")
    
    # Test creating output directory
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    print(f"  ✓ Output directory ready: {output_dir.absolute()}")
    
except Exception as e:
    print(f"✗ File operations test failed: {e}")

# Test 7: Import the main script
print("\n7. Testing main script import...")
try:
    import street_account_news_canadian_banks as main_script
    print("  ✓ street_account_news_canadian_banks imported")
    
    # Check for expected functions
    expected_functions = [
        'setup_logging',
        'validate_environment_variables',
        'get_nas_connection',
        'setup_ssl_certificate',
        'setup_proxy_configuration',
        'setup_factset_api_client',
        'get_news_for_ticker',
        'main'
    ]
    
    for func in expected_functions:
        if hasattr(main_script, func):
            print(f"    - {func}: ✓")
        else:
            print(f"    - {func}: ✗ (missing)")
    
except ImportError as e:
    print(f"✗ Main script import failed: {e}")

print("\n" + "=" * 60)
print("IMPORT TESTING COMPLETE")
print("=" * 60)

print("\n📋 Next steps:")
print("1. Copy .env.template to .env and fill in your credentials")
print("2. Ensure config.yaml is properly configured")
print("3. Run with --local flag for local testing:")
print("   python street_account_news_canadian_banks.py --local")
print("4. Remove --local flag when ready to connect to NAS")
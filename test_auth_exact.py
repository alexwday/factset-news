#!/usr/bin/env python3
"""
Test authentication EXACTLY as the working script does it
This matches street_account_news_canadian_banks.py precisely
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("="*60)
print("TESTING FACTSET API - EXACT MATCH TO WORKING SCRIPT")
print("="*60)

# Import exactly as working script does
import fds.sdk.StreetAccountNews
from fds.sdk.StreetAccountNews.api import headlines_api
from fds.sdk.StreetAccountNews.models import *

# Check environment variables
api_username = os.getenv("API_USERNAME")
api_password = os.getenv("API_PASSWORD")

print(f"\n1. Environment variables:")
print(f"   API_USERNAME: {'set' if api_username else 'NOT SET'}")
print(f"   API_PASSWORD: {'set' if api_password else 'NOT SET'}")

if not api_username or not api_password:
    print("\n❌ API credentials not found in environment")
    sys.exit(1)

# Test 1: LOCAL mode (like --local flag)
print("\n2. Testing LOCAL mode configuration (like --local flag):")
print("   Creating configuration WITHOUT get_basic_auth_token()...")

try:
    # EXACTLY as in the working script's LOCAL mode (lines 1093-1096)
    api_configuration = fds.sdk.StreetAccountNews.Configuration(
        username=os.getenv("API_USERNAME", "test_user"),
        password=os.getenv("API_PASSWORD", "test_pass"),
    )
    # NOTE: NO get_basic_auth_token() call here!
    
    print("   ✓ Configuration created (LOCAL mode)")
    
    # Test API call - exactly as working script
    with fds.sdk.StreetAccountNews.ApiClient(api_configuration) as api_client:
        headlines_api_instance = headlines_api.HeadlinesApi(api_client)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        # Build request exactly as working script
        headlines_request_data = HeadlinesRequestData(
            tickers=[
                HeadlinesRequestTickersObject(
                    value="RY-CA",
                    type="Equity"
                )
            ],
            search_time=HeadlinesRequestDataSearchTime(
                start=start_date,
                end=end_date
            )
        )
        
        headlines_request = HeadlinesRequest(
            data=headlines_request_data,
            meta=HeadlinesRequestMeta(
                pagination=HeadlinesRequestMetaPagination(
                    limit=5,
                    offset=0
                ),
                attributes=["headlines", "storyTime", "id", "primarySymbols", "symbols", "subjects", "storyBody", "url"]
            )
        )
        
        print("   Making API call (LOCAL mode)...")
        response = headlines_api_instance.get_street_account_headlines(
            headlines_request=headlines_request
        )
        
        count = len(response.data) if response and response.data else 0
        print(f"   ✅ LOCAL mode works! Found {count} headlines")
        
except Exception as e:
    print(f"   ❌ LOCAL mode failed: {e}")
    error_details = str(e)
    if "403" in error_details:
        print("   403 Error in LOCAL mode")

# Test 2: With get_basic_auth_token() 
print("\n3. Testing with get_basic_auth_token() (like non-local mode):")

try:
    # Like non-local mode (lines 468-476)
    configuration2 = fds.sdk.StreetAccountNews.Configuration(
        username=api_username,
        password=api_password,
        # Note: no proxy or ssl_ca_cert in test
    )
    
    # This is what non-local mode does
    configuration2.get_basic_auth_token()
    print("   ✓ Configuration created with auth token")
    
    with fds.sdk.StreetAccountNews.ApiClient(configuration2) as api_client:
        headlines_api_instance = headlines_api.HeadlinesApi(api_client)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        headlines_request = HeadlinesRequest(
            data=HeadlinesRequestData(
                tickers=[
                    HeadlinesRequestTickersObject(value="RY-CA", type="Equity")
                ],
                search_time=HeadlinesRequestDataSearchTime(
                    start=start_date,
                    end=end_date
                )
            ),
            meta=HeadlinesRequestMeta(
                pagination=HeadlinesRequestMetaPagination(limit=5, offset=0),
                attributes=["headlines", "storyTime", "id"]
            )
        )
        
        print("   Making API call (with auth token)...")
        response = headlines_api_instance.get_street_account_headlines(
            headlines_request=headlines_request
        )
        
        count = len(response.data) if response and response.data else 0
        print(f"   ✅ Auth token mode works! Found {count} headlines")
        
except Exception as e:
    print(f"   ❌ Auth token mode failed: {e}")
    if "403" in str(e):
        print("   403 Error with auth token")

print("\n" + "="*60)
print("RESULTS:")
print("="*60)
print("\nThe working script runs differently based on --local flag:")
print("• WITH --local: No get_basic_auth_token() call")
print("• WITHOUT --local: Calls get_basic_auth_token() + uses proxy/SSL")
print("\nRun your working script and check if you use --local flag")
print("That will tell us which authentication method actually works for you")
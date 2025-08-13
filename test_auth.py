#!/usr/bin/env python3
"""
Test authentication with FactSet Street Account News API
Simple script to verify credentials and connection
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("="*60)
print("TESTING FACTSET API AUTHENTICATION")
print("="*60)

# Check environment variables
print("\n1. Checking environment variables...")
api_username = os.getenv("API_USERNAME")
api_password = os.getenv("API_PASSWORD")

if not api_username:
    print("❌ API_USERNAME not found in .env")
    sys.exit(1)
else:
    print(f"✓ API_USERNAME: {api_username[:3]}...")

if not api_password:
    print("❌ API_PASSWORD not found in .env")
    sys.exit(1)
else:
    print(f"✓ API_PASSWORD: ***hidden***")

# Try to import the SDK
print("\n2. Importing FactSet SDK...")
try:
    import fds.sdk.StreetAccountNews
    from fds.sdk.StreetAccountNews.api import headlines_api
    from fds.sdk.StreetAccountNews.models import *
    print("✓ SDK imported successfully")
except ImportError as e:
    print(f"❌ Failed to import SDK: {e}")
    sys.exit(1)

# Create configuration
print("\n3. Creating API configuration...")
try:
    configuration = fds.sdk.StreetAccountNews.Configuration(
        username=api_username,
        password=api_password
    )
    print("✓ Configuration created")
    
    # CRITICAL: Generate the auth token!
    print("\n4. Generating authentication token...")
    configuration.get_basic_auth_token()
    print("✓ Auth token generated")
    
except Exception as e:
    print(f"❌ Failed to create configuration: {e}")
    sys.exit(1)

# Test API connection with minimal request
print("\n5. Testing API connection...")
print("   Making a simple request for RY-CA news (last 1 day)...")

try:
    with fds.sdk.StreetAccountNews.ApiClient(configuration) as api_client:
        # Create API instance
        api_instance = headlines_api.HeadlinesApi(api_client)
        
        # Create minimal request
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        request = HeadlinesRequest(
            data=HeadlinesRequestData(
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
            ),
            meta=HeadlinesRequestMeta(
                pagination=HeadlinesRequestMetaPagination(
                    limit=5,
                    offset=0
                ),
                attributes=["headlines", "storyTime", "id"]
            )
        )
        
        # Make API call
        print("   Sending request...")
        response = api_instance.get_street_account_headlines(
            headlines_request=request
        )
        
        # Check response
        if response and hasattr(response, 'data'):
            count = len(response.data) if response.data else 0
            print(f"✓ API call successful! Found {count} headlines")
            
            if count > 0 and hasattr(response.data[0], 'headlines'):
                print(f"\n   Sample headline: {response.data[0].headlines[:80]}...")
        else:
            print("✓ API call successful but no data returned")
    
    print("\n" + "="*60)
    print("✅ AUTHENTICATION TEST PASSED")
    print("="*60)
    print("\nYour credentials are working correctly!")
    print("The 403 error might be due to:")
    print("1. Rate limiting - try waiting a few minutes")
    print("2. Missing proxy/SSL setup if required by your network")
    print("3. Specific endpoint restrictions")
    
except Exception as e:
    print(f"\n❌ API call failed: {e}")
    print("\n" + "="*60)
    print("AUTHENTICATION TEST FAILED")
    print("="*60)
    
    error_str = str(e).lower()
    
    if "403" in error_str or "forbidden" in error_str:
        print("\n⚠️  403 Forbidden Error - Possible causes:")
        print("1. Invalid API credentials")
        print("2. API credentials lack permissions for Street Account News")
        print("3. Rate limiting - too many requests")
        print("4. IP address not whitelisted")
        print("5. Missing required headers or proxy configuration")
    elif "401" in error_str or "unauthorized" in error_str:
        print("\n⚠️  401 Unauthorized - Check your API credentials")
    elif "connection" in error_str or "timeout" in error_str:
        print("\n⚠️  Connection error - Check network/proxy settings")
    else:
        print(f"\n⚠️  Unexpected error: {e}")
    
    print("\nTroubleshooting steps:")
    print("1. Verify API_USERNAME and API_PASSWORD in .env")
    print("2. Check if you need proxy settings (PROXY_USER, PROXY_PASSWORD, PROXY_URL)")
    print("3. Wait 5-10 minutes if rate limited")
    print("4. Contact FactSet support to verify API access")
    
    sys.exit(1)
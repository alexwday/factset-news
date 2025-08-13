#!/usr/bin/env python3
"""
Test authentication WITH proxy and SSL - matching the working script exactly
This is what street_account_news_canadian_banks.py does when NOT using --local
"""

import os
import sys
import tempfile
from datetime import datetime, timedelta
from urllib.parse import quote
from dotenv import load_dotenv
from smb.SMBConnection import SMBConnection
import io

# Load environment variables
load_dotenv()

print("="*60)
print("TESTING WITH FULL PROXY AND SSL SETUP")
print("="*60)

# Import exactly as working script
import fds.sdk.StreetAccountNews
from fds.sdk.StreetAccountNews.api import headlines_api
from fds.sdk.StreetAccountNews.models import *

# Step 1: Validate environment variables (same as working script)
print("\n1. Checking environment variables...")
required_env_vars = [
    "API_USERNAME",
    "API_PASSWORD",
    "PROXY_USER",
    "PROXY_PASSWORD",
    "PROXY_URL",
    "NAS_USERNAME",
    "NAS_PASSWORD",
    "NAS_SERVER_IP",
    "NAS_SERVER_NAME",
    "NAS_SHARE_NAME",
]

missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
    print("\nThe working script requires ALL of these when not using --local")
    sys.exit(1)

print("✓ All required environment variables present")

# Step 2: Set up SSL certificate (simplified - not from NAS for testing)
print("\n2. Setting up SSL certificate...")
ssl_cert_path = None

# Check if we can get cert from NAS
try:
    conn = SMBConnection(
        username=os.getenv("NAS_USERNAME"),
        password=os.getenv("NAS_PASSWORD"),
        my_name=os.getenv("CLIENT_MACHINE_NAME", "TEST-CLIENT"),
        remote_name=os.getenv("NAS_SERVER_NAME"),
        use_ntlm_v2=True,
        is_direct_tcp=True,
    )
    
    nas_port = int(os.getenv("NAS_PORT", 445))
    if conn.connect(os.getenv("NAS_SERVER_IP"), nas_port):
        print("✓ Connected to NAS")
        
        # Try to get SSL cert
        cert_path = "Finance Data and Analytics/DSA/Earnings Call Transcripts/Inputs/Certificate/rbc-ca-bundle.cer"
        try:
            file_obj = io.BytesIO()
            conn.retrieveFile(os.getenv("NAS_SHARE_NAME"), cert_path, file_obj)
            file_obj.seek(0)
            cert_data = file_obj.read()
            
            # Create temp cert file
            temp_cert = tempfile.NamedTemporaryFile(mode="wb", suffix=".cer", delete=False)
            temp_cert.write(cert_data)
            temp_cert.close()
            ssl_cert_path = temp_cert.name
            
            # Set environment variables
            os.environ["REQUESTS_CA_BUNDLE"] = ssl_cert_path
            os.environ["SSL_CERT_FILE"] = ssl_cert_path
            
            print(f"✓ SSL certificate configured: {ssl_cert_path}")
        except Exception as e:
            print(f"⚠ Could not get SSL cert from NAS: {e}")
        
        conn.close()
    else:
        print("⚠ Could not connect to NAS")
        
except Exception as e:
    print(f"⚠ NAS connection failed: {e}")
    print("  Continuing without SSL certificate...")

# Step 3: Configure proxy (exactly as working script)
print("\n3. Configuring proxy...")
proxy_user = os.getenv("PROXY_USER")
proxy_password = os.getenv("PROXY_PASSWORD")
proxy_url = os.getenv("PROXY_URL")
proxy_domain = os.getenv("PROXY_DOMAIN", "MAPLE")

# Escape domain and user for NTLM authentication (same as working script)
escaped_domain = quote(proxy_domain + "\\" + proxy_user)
quoted_password = quote(proxy_password)

# Construct proxy URL
proxy_url_formatted = f"http://{escaped_domain}:{quoted_password}@{proxy_url}"
print(f"✓ Proxy configured for {proxy_url}")

# Step 4: Set up FactSet API client (exactly as working script lines 468-476)
print("\n4. Setting up FactSet API client...")
try:
    api_username = os.getenv("API_USERNAME")
    api_password = os.getenv("API_PASSWORD")
    
    # EXACTLY as working script does it
    configuration = fds.sdk.StreetAccountNews.Configuration(
        username=api_username,
        password=api_password,
        proxy=proxy_url_formatted,
        ssl_ca_cert=ssl_cert_path,
    )
    
    # Generate authentication token (line 476)
    configuration.get_basic_auth_token()
    
    print("✓ API client configured with proxy and SSL")
    
    # Step 5: Test API call
    print("\n5. Testing API call...")
    with fds.sdk.StreetAccountNews.ApiClient(configuration) as api_client:
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
        
        print("   Making API call...")
        response = headlines_api_instance.get_street_account_headlines(
            headlines_request=headlines_request
        )
        
        count = len(response.data) if response and response.data else 0
        print(f"\n✅ SUCCESS! Found {count} headlines")
        
        if count > 0 and hasattr(response.data[0], 'headlines'):
            print(f"   Sample: {response.data[0].headlines[:80]}...")
    
    print("\n" + "="*60)
    print("AUTHENTICATION SUCCESSFUL")
    print("="*60)
    print("\nYour setup requires:")
    print("1. SSL certificate from NAS")
    print("2. Proxy configuration with NTLM auth")
    print("3. get_basic_auth_token() call")
    print("\nAll three are needed for authentication to work!")
    
except Exception as e:
    print(f"\n❌ API call failed: {e}")
    print("\n" + "="*60)
    print("AUTHENTICATION FAILED")
    print("="*60)
    
    error_str = str(e).lower()
    if "403" in error_str:
        print("\nThe 403 error means one of:")
        print("1. Missing proxy configuration")
        print("2. Missing SSL certificate")
        print("3. Incorrect proxy credentials")
        print("4. Network blocking without proxy")
    
    print("\nYour working script uses:")
    print("• SSL cert from NAS")
    print("• Proxy with NTLM authentication")
    print("• Both are REQUIRED for your network")

finally:
    # Cleanup
    if ssl_cert_path and os.path.exists(ssl_cert_path):
        try:
            os.unlink(ssl_cert_path)
            print("\n✓ Cleaned up temporary SSL certificate")
        except:
            pass
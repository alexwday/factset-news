#!/usr/bin/env python3
"""
Test filter values to understand what the API accepts
"""

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import fds.sdk.StreetAccountNews
from fds.sdk.StreetAccountNews.api import headlines_api, filters_api
from fds.sdk.StreetAccountNews.models import *

load_dotenv()


def test_filters():
    """Test what filter values work"""
    
    print("="*60)
    print("TESTING FILTER VALUES")
    print("="*60)
    
    # Setup API
    api_username = os.getenv("API_USERNAME")
    api_password = os.getenv("API_PASSWORD")
    
    if not api_username or not api_password:
        print("ERROR: Set API_USERNAME and API_PASSWORD in .env")
        return
    
    # Setup proxy (required for your network)
    proxy_url = None
    if os.getenv("PROXY_USER") and os.getenv("PROXY_URL"):
        from urllib.parse import quote
        proxy_user = os.getenv("PROXY_USER")
        proxy_password = os.getenv("PROXY_PASSWORD")
        proxy_url_raw = os.getenv("PROXY_URL")
        proxy_domain = os.getenv("PROXY_DOMAIN", "MAPLE")
        
        escaped_domain = quote(proxy_domain + "\\" + proxy_user)
        quoted_password = quote(proxy_password)
        proxy_url = f"http://{escaped_domain}:{quoted_password}@{proxy_url_raw}"
        print("✓ Proxy configured")
    
    configuration = fds.sdk.StreetAccountNews.Configuration(
        username=api_username,
        password=api_password,
        proxy=proxy_url  # Add proxy
    )
    
    # CRITICAL: Generate the auth token!
    configuration.get_basic_auth_token()
    
    with fds.sdk.StreetAccountNews.ApiClient(configuration) as api_client:
        filters_api_instance = filters_api.FiltersApi(api_client)
        headlines_api_instance = headlines_api.HeadlinesApi(api_client)
        
        # 1. Get the actual filter values from the API
        print("\n1. FETCHING AVAILABLE FILTERS FROM API")
        print("-"*40)
        
        try:
            # Get sectors
            print("\n📊 Available Sectors:")
            sectors_response = filters_api_instance.get_street_account_filters_sectors()
            if sectors_response and hasattr(sectors_response, 'data'):
                if hasattr(sectors_response.data, 'sectors'):
                    for sector in sectors_response.data.sectors[:20]:
                        if hasattr(sector, 'name'):
                            print(f"  - {sector.name}")
                            # Check for ID or code
                            if hasattr(sector, 'id'):
                                print(f"    (ID: {sector.id})")
                            if hasattr(sector, 'code'):
                                print(f"    (Code: {sector.code})")
        except Exception as e:
            print(f"  Error fetching sectors: {e}")
        
        try:
            # Get regions  
            print("\n🌍 Available Regions:")
            regions_response = filters_api_instance.get_street_account_filters_regions()
            if regions_response and hasattr(regions_response, 'data'):
                if hasattr(regions_response.data, 'regions'):
                    for region in regions_response.data.regions:
                        if hasattr(region, 'name'):
                            print(f"  - {region.name}")
                            # Check for ID or code
                            if hasattr(region, 'id'):
                                print(f"    (ID: {region.id})")
                            if hasattr(region, 'code'):
                                print(f"    (Code: {region.code})")
        except Exception as e:
            print(f"  Error fetching regions: {e}")
        
        try:
            # Get categories
            print("\n📁 Available Categories (first 20):")
            categories_response = filters_api_instance.get_street_account_filters_categories()
            if categories_response and hasattr(categories_response, 'data'):
                if hasattr(categories_response.data, 'categories'):
                    for i, category in enumerate(categories_response.data.categories[:20], 1):
                        if hasattr(category, 'name'):
                            print(f"  {i}. {category.name}")
        except Exception as e:
            print(f"  Error fetching categories: {e}")
        
        # 2. Test a working search (ticker only)
        print("\n\n2. TESTING WORKING SEARCH (Ticker only)")
        print("-"*40)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        try:
            request = HeadlinesRequest(
                data=HeadlinesRequestData(
                    tickers=[HeadlinesRequestTickersObject(value="RY-CA", type="Equity")],
                    search_time=HeadlinesRequestDataSearchTime(start=start_date, end=end_date)
                ),
                meta=HeadlinesRequestMeta(
                    pagination=HeadlinesRequestMetaPagination(limit=5, offset=0),
                    attributes=["headlines", "id"]
                )
            )
            
            response = headlines_api_instance.get_street_account_headlines(headlines_request=request)
            count = len(response.data) if response and response.data else 0
            print(f"✓ Ticker search works: {count} results")
            
        except Exception as e:
            print(f"✗ Ticker search failed: {e}")
        
        # 3. Test sector search with exact value from API
        print("\n3. TESTING SECTOR SEARCH")
        print("-"*40)
        print("Instructions: Copy exact sector name from above and test it")
        
        # Try to get first sector from API response
        test_sector = None
        try:
            sectors_response = filters_api_instance.get_street_account_filters_sectors()
            if sectors_response and hasattr(sectors_response.data, 'sectors'):
                if sectors_response.data.sectors:
                    first_sector = sectors_response.data.sectors[0]
                    if hasattr(first_sector, 'name'):
                        test_sector = first_sector.name
                        print(f"Testing with sector: '{test_sector}'")
        except:
            pass
        
        if test_sector:
            try:
                request = HeadlinesRequest(
                    data=HeadlinesRequestData(
                        sectors=[test_sector],
                        search_time=HeadlinesRequestDataSearchTime(start=start_date, end=end_date)
                    ),
                    meta=HeadlinesRequestMeta(
                        pagination=HeadlinesRequestMetaPagination(limit=5, offset=0),
                        attributes=["headlines", "id"]
                    )
                )
                
                response = headlines_api_instance.get_street_account_headlines(headlines_request=request)
                count = len(response.data) if response and response.data else 0
                print(f"✓ Sector search works: {count} results")
                
            except Exception as e:
                print(f"✗ Sector search failed: {e}")
                print(f"  Error details: {str(e)[:500]}")
        
        # 4. Test region search
        print("\n4. TESTING REGION SEARCH")
        print("-"*40)
        
        test_region = None
        try:
            regions_response = filters_api_instance.get_street_account_filters_regions()
            if regions_response and hasattr(regions_response.data, 'regions'):
                if regions_response.data.regions:
                    # Look for Canada or take first
                    for region in regions_response.data.regions:
                        if hasattr(region, 'name'):
                            if 'canada' in region.name.lower():
                                test_region = region.name
                                break
                    if not test_region and regions_response.data.regions:
                        first_region = regions_response.data.regions[0]
                        if hasattr(first_region, 'name'):
                            test_region = first_region.name
                    
                    if test_region:
                        print(f"Testing with region: '{test_region}'")
        except:
            pass
        
        if test_region:
            try:
                request = HeadlinesRequest(
                    data=HeadlinesRequestData(
                        regions=[test_region],
                        search_time=HeadlinesRequestDataSearchTime(start=start_date, end=end_date)
                    ),
                    meta=HeadlinesRequestMeta(
                        pagination=HeadlinesRequestMetaPagination(limit=5, offset=0),
                        attributes=["headlines", "id"]
                    )
                )
                
                response = headlines_api_instance.get_street_account_headlines(headlines_request=request)
                count = len(response.data) if response and response.data else 0
                print(f"✓ Region search works: {count} results")
                
            except Exception as e:
                print(f"✗ Region search failed: {e}")
                print(f"  Error details: {str(e)[:500]}")
        
        print("\n" + "="*60)
        print("FILTER TEST COMPLETE")
        print("="*60)
        print("\nKey findings:")
        print("1. Check the exact filter values listed above")
        print("2. The API may require exact matches (case-sensitive)")
        print("3. Some filters might need IDs instead of names")
        print("4. Check if any banking/Canada filters exist")


if __name__ == "__main__":
    test_filters()
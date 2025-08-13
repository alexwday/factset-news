"""
Street Account News Explorer - Comprehensive news search for Canadian banks
This script explores different ways to get more comprehensive news coverage
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
import pandas as pd
from pathlib import Path

import yaml
import fds.sdk.StreetAccountNews
from fds.sdk.StreetAccountNews.api import headlines_api, filters_api
from fds.sdk.StreetAccountNews.models import *
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def setup_api_client():
    """Set up the API client for testing"""
    # For local testing - simplified setup
    api_username = os.getenv("API_USERNAME", "test_user")
    api_password = os.getenv("API_PASSWORD", "test_pass")
    
    configuration = fds.sdk.StreetAccountNews.Configuration(
        username=api_username,
        password=api_password,
    )
    
    # CRITICAL: Generate the auth token!
    configuration.get_basic_auth_token()
    
    return configuration


def explore_available_filters(api_client):
    """Explore all available filters to understand what's possible"""
    filters_api_instance = filters_api.FiltersApi(api_client)
    
    print("\n" + "="*60)
    print("EXPLORING AVAILABLE FILTERS")
    print("="*60)
    
    try:
        # Get ALL filters
        response = filters_api_instance.get_street_account_filters(
            structured=True,
            flattened=True
        )
        
        if response and hasattr(response, 'data'):
            data = response.data
            
            # Extract flattened filters
            if hasattr(data, 'flattened_filters'):
                flattened = data.flattened_filters
                
                # Categories
                if hasattr(flattened, 'categories') and flattened.categories:
                    categories = [cat.name for cat in flattened.categories if hasattr(cat, 'name')]
                    print(f"\nCategories ({len(categories)}):")
                    for i, cat in enumerate(categories[:20], 1):  # Show first 20
                        print(f"  {i}. {cat}")
                    if len(categories) > 20:
                        print(f"  ... and {len(categories)-20} more")
                
                # Topics
                if hasattr(flattened, 'topics') and flattened.topics:
                    topics = [topic.name for topic in flattened.topics if hasattr(topic, 'name')]
                    print(f"\nTopics ({len(topics)}):")
                    for i, topic in enumerate(topics[:20], 1):  # Show first 20
                        print(f"  {i}. {topic}")
                    if len(topics) > 20:
                        print(f"  ... and {len(topics)-20} more")
                
                # Regions
                if hasattr(flattened, 'regions') and flattened.regions:
                    regions = [region.name for region in flattened.regions if hasattr(region, 'name')]
                    print(f"\nRegions ({len(regions)}):")
                    for i, region in enumerate(regions, 1):
                        print(f"  {i}. {region}")
                
                # Sectors
                if hasattr(flattened, 'sectors') and flattened.sectors:
                    sectors = [sector.name for sector in flattened.sectors if hasattr(sector, 'name')]
                    print(f"\nSectors ({len(sectors)}):")
                    for i, sector in enumerate(sectors, 1):
                        print(f"  {i}. {sector}")
                
                # Watchlists
                if hasattr(flattened, 'watchlists') and flattened.watchlists:
                    watchlists = [wl.name for wl in flattened.watchlists if hasattr(wl, 'name')]
                    print(f"\nWatchlists ({len(watchlists)}):")
                    for i, wl in enumerate(watchlists[:10], 1):  # Show first 10
                        print(f"  {i}. {wl}")
                    if len(watchlists) > 10:
                        print(f"  ... and {len(watchlists)-10} more")
                        
    except Exception as e:
        print(f"Error exploring filters: {e}")


def search_multiple_ways(api_client, ticker: str, bank_name: str):
    """Try multiple search approaches to get comprehensive news"""
    headlines_api_instance = headlines_api.HeadlinesApi(api_client)
    
    print(f"\n" + "="*60)
    print(f"SEARCHING FOR {ticker} - {bank_name}")
    print("="*60)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)  # Last 7 days for testing
    results = {}
    
    # Method 1: Search by ticker only (no filters)
    print("\n1. Search by ticker only (no filters)...")
    try:
        request = HeadlinesRequest(
            data=HeadlinesRequestData(
                tickers=[
                    HeadlinesRequestTickersObject(
                        value=ticker,
                        type="Equity"
                    )
                ],
                search_time=HeadlinesRequestDataSearchTime(
                    start=start_date,
                    end=end_date
                )
            ),
            meta=HeadlinesRequestMeta(
                pagination=HeadlinesRequestMetaPagination(limit=100, offset=0),
                attributes=["headlines", "storyTime", "id", "primarySymbols", "symbols", "subjects", "storyBody"]
            )
        )
        
        response = headlines_api_instance.get_street_account_headlines(headlines_request=request)
        count = len(response.data) if response and response.data else 0
        results['ticker_only'] = count
        print(f"   Found {count} headlines")
        
        # Show sample headlines
        if response and response.data and len(response.data) > 0:
            print("   Sample headlines:")
            for item in response.data[:3]:
                if hasattr(item, 'headlines'):
                    print(f"   - {item.headlines[:100]}...")
                    
    except Exception as e:
        print(f"   Error: {e}")
        results['ticker_only'] = 0
    
    # Method 2: Search with Banking sector filter
    print("\n2. Search by ticker + Banking sector...")
    try:
        request = HeadlinesRequest(
            data=HeadlinesRequestData(
                tickers=[
                    HeadlinesRequestTickersObject(
                        value=ticker,
                        type="Equity"
                    )
                ],
                sectors=["Banking"],
                search_time=HeadlinesRequestDataSearchTime(
                    start=start_date,
                    end=end_date
                )
            ),
            meta=HeadlinesRequestMeta(
                pagination=HeadlinesRequestMetaPagination(limit=100, offset=0),
                attributes=["headlines", "storyTime", "id", "primarySymbols", "symbols"]
            )
        )
        
        response = headlines_api_instance.get_street_account_headlines(headlines_request=request)
        count = len(response.data) if response and response.data else 0
        results['ticker_banking'] = count
        print(f"   Found {count} headlines")
        
    except Exception as e:
        print(f"   Error: {e}")
        results['ticker_banking'] = 0
    
    # Method 3: Search with Canada region filter  
    print("\n3. Search by ticker + Canada region...")
    try:
        request = HeadlinesRequest(
            data=HeadlinesRequestData(
                tickers=[
                    HeadlinesRequestTickersObject(
                        value=ticker,
                        type="Equity"
                    )
                ],
                regions=["Canada"],
                search_time=HeadlinesRequestDataSearchTime(
                    start=start_date,
                    end=end_date
                )
            ),
            meta=HeadlinesRequestMeta(
                pagination=HeadlinesRequestMetaPagination(limit=100, offset=0),
                attributes=["headlines", "storyTime", "id"]
            )
        )
        
        response = headlines_api_instance.get_street_account_headlines(headlines_request=request)
        count = len(response.data) if response and response.data else 0
        results['ticker_canada'] = count
        print(f"   Found {count} headlines")
        
    except Exception as e:
        print(f"   Error: {e}")
        results['ticker_canada'] = 0
    
    # Method 4: Search WITHOUT ticker - just Banking sector
    print("\n4. Search Banking sector (no ticker)...")
    try:
        request = HeadlinesRequest(
            data=HeadlinesRequestData(
                sectors=["Banking"],
                search_time=HeadlinesRequestDataSearchTime(
                    start=start_date,
                    end=end_date
                )
            ),
            meta=HeadlinesRequestMeta(
                pagination=HeadlinesRequestMetaPagination(limit=100, offset=0),
                attributes=["headlines", "storyTime", "id", "symbols"]
            )
        )
        
        response = headlines_api_instance.get_street_account_headlines(headlines_request=request)
        count = len(response.data) if response and response.data else 0
        results['banking_only'] = count
        print(f"   Found {count} headlines")
        
        # Check how many mention our ticker
        if response and response.data:
            mentions = 0
            for item in response.data:
                if hasattr(item, 'symbols') and item.symbols:
                    if ticker in item.symbols:
                        mentions += 1
            print(f"   {mentions} mention {ticker}")
            
    except Exception as e:
        print(f"   Error: {e}")
        results['banking_only'] = 0
    
    # Method 5: Search with Financial sector (broader)
    print("\n5. Search by ticker + Financial sector...")
    try:
        request = HeadlinesRequest(
            data=HeadlinesRequestData(
                tickers=[
                    HeadlinesRequestTickersObject(
                        value=ticker,
                        type="Equity"
                    )
                ],
                sectors=["Financial"],
                search_time=HeadlinesRequestDataSearchTime(
                    start=start_date,
                    end=end_date
                )
            ),
            meta=HeadlinesRequestMeta(
                pagination=HeadlinesRequestMetaPagination(limit=100, offset=0),
                attributes=["headlines", "storyTime", "id"]
            )
        )
        
        response = headlines_api_instance.get_street_account_headlines(headlines_request=request)
        count = len(response.data) if response and response.data else 0
        results['ticker_financial'] = count
        print(f"   Found {count} headlines")
        
    except Exception as e:
        print(f"   Error: {e}")
        results['ticker_financial'] = 0
    
    # Method 6: Try alternative ticker formats
    alt_tickers = [
        ticker.replace("-CA", ""),  # Try without -CA
        ticker.replace("-", "."),    # Try with dot
        f"{ticker.split('-')[0]}:CN", # Try Bloomberg format
        ticker.split("-")[0]         # Just the base ticker
    ]
    
    print(f"\n6. Testing alternative ticker formats...")
    for alt_ticker in alt_tickers:
        try:
            request = HeadlinesRequest(
                data=HeadlinesRequestData(
                    tickers=[
                        HeadlinesRequestTickersObject(
                            value=alt_ticker,
                            type="Equity"
                        )
                    ],
                    search_time=HeadlinesRequestDataSearchTime(
                        start=start_date,
                        end=end_date
                    )
                ),
                meta=HeadlinesRequestMeta(
                    pagination=HeadlinesRequestMetaPagination(limit=10, offset=0),
                    attributes=["headlines", "id"]
                )
            )
            
            response = headlines_api_instance.get_street_account_headlines(headlines_request=request)
            count = len(response.data) if response and response.data else 0
            print(f"   {alt_ticker}: {count} headlines")
            results[f'alt_{alt_ticker}'] = count
            
        except Exception as e:
            print(f"   {alt_ticker}: Error - {str(e)[:50]}...")
            results[f'alt_{alt_ticker}'] = 0
    
    return results


def main():
    """Main function to explore Street Account News API"""
    
    print("\n" + "="*60)
    print("STREET ACCOUNT NEWS API EXPLORER")
    print("="*60)
    
    # Setup API
    configuration = setup_api_client()
    
    with fds.sdk.StreetAccountNews.ApiClient(configuration) as api_client:
        
        # First, explore available filters
        explore_available_filters(api_client)
        
        # Test different search methods for Canadian banks
        canadian_banks = {
            "RY-CA": "Royal Bank of Canada",
            "TD-CA": "Toronto-Dominion Bank",
            "BMO-CA": "Bank of Montreal"
        }
        
        all_results = {}
        for ticker, name in canadian_banks.items():
            results = search_multiple_ways(api_client, ticker, name)
            all_results[ticker] = results
        
        # Summary
        print("\n" + "="*60)
        print("SUMMARY OF RESULTS")
        print("="*60)
        
        df = pd.DataFrame(all_results).T
        print("\n", df.to_string())
        
        # Save results
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        df.to_csv(output_dir / f"news_exploration_{timestamp}.csv")
        print(f"\nResults saved to output/news_exploration_{timestamp}.csv")
        
        # Recommendations
        print("\n" + "="*60)
        print("RECOMMENDATIONS")
        print("="*60)
        print("\nBased on the results above:")
        print("1. Check which search method returns the most results")
        print("2. Consider using broader filters (Financial vs Banking)")
        print("3. Test alternative ticker formats if standard format gives limited results")
        print("4. Consider searching without tickers and filtering results locally")
        print("5. May need to combine multiple search approaches for comprehensive coverage")


if __name__ == "__main__":
    main()
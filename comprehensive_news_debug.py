"""
Comprehensive Canadian Banking News - DEBUG VERSION
Enhanced error logging to diagnose bad request issues
"""

import os
import tempfile
import json
import time
from datetime import datetime, timedelta
from urllib.parse import quote
from typing import Dict, Any, List, Set
import pandas as pd
from pathlib import Path
import traceback

import yaml
import fds.sdk.StreetAccountNews
from fds.sdk.StreetAccountNews.api import headlines_api, filters_api
from fds.sdk.StreetAccountNews.models import *
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class CanadianBankingNewsCollector:
    """Comprehensive news collector with debug logging"""
    
    def __init__(self, api_client):
        self.api_client = api_client
        self.headlines_api = headlines_api.HeadlinesApi(api_client)
        self.filters_api = filters_api.FiltersApi(api_client)
        
        # Canadian bank tickers
        self.canadian_banks = {
            "RY-CA": {"name": "Royal Bank of Canada"},
            "TD-CA": {"name": "Toronto-Dominion Bank"},
            "BMO-CA": {"name": "Bank of Montreal"},
            "BNS-CA": {"name": "Bank of Nova Scotia"},
            "CM-CA": {"name": "CIBC"},
            "NA-CA": {"name": "National Bank"},
            "LB-CA": {"name": "Laurentian Bank"}
        }
        
        self.all_news = []
        self.unique_stories = set()
        self.request_count = 0
        self.request_delay = 2.0
        
        # Debug mode
        self.debug = True
        self.debug_log = []
    
    def collect_all_news(self, days_back: int = 3) -> pd.DataFrame:
        """Collect news using multiple strategies"""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        print(f"\nCollecting news from {start_date.date()} to {end_date.date()}")
        print("="*60)
        
        # First, test what filters are available
        print("\n🔍 CHECKING AVAILABLE FILTERS")
        print("-"*40)
        self._check_available_filters()
        
        # Strategy 1: Direct ticker search (WORKING)
        print("\n📊 STRATEGY 1: Direct ticker searches")
        print("-"*40)
        for ticker, info in self.canadian_banks.items():
            self._rate_limit()
            self._test_search(
                search_type="TICKER",
                tickers=[ticker],
                description=f"{ticker} ({info['name']})"
            )
            # Just test first 2 banks to save API calls
            if self.request_count >= 2:
                break
        
        # Strategy 2: Sector search (FAILING)
        print("\n🏦 STRATEGY 2: Testing sector searches")
        print("-"*40)
        
        # Test different sector values
        sectors_to_test = [
            "Banking",
            "Financial",
            "Financials",
            "Banks",
            "BANKING",
            "banking"
        ]
        
        for sector in sectors_to_test:
            self._rate_limit()
            success = self._test_search(
                search_type="SECTOR",
                sectors=[sector],
                description=f"Sector: '{sector}'"
            )
            if success:
                print(f"  ✓ Sector '{sector}' works!")
                break
        
        # Strategy 3: Region search (FAILING)
        print("\n🌍 STRATEGY 3: Testing region searches")
        print("-"*40)
        
        # Test different region values
        regions_to_test = [
            "Canada",
            "North America",
            "Americas",
            "CANADA",
            "canada",
            "CA"
        ]
        
        for region in regions_to_test:
            self._rate_limit()
            success = self._test_search(
                search_type="REGION",
                regions=[region],
                description=f"Region: '{region}'"
            )
            if success:
                print(f"  ✓ Region '{region}' works!")
                break
        
        # Strategy 4: Combined search
        print("\n🔄 STRATEGY 4: Testing combined searches")
        print("-"*40)
        
        # Test ticker + sector
        self._rate_limit()
        self._test_search(
            search_type="TICKER+SECTOR",
            tickers=["RY-CA"],
            sectors=["Financial"],
            description="RY-CA + Financial sector"
        )
        
        # Print debug log
        if self.debug_log:
            print("\n" + "="*60)
            print("DEBUG LOG")
            print("="*60)
            for entry in self.debug_log:
                print(f"\n{entry}")
        
        # Convert to DataFrame
        if self.all_news:
            df = pd.DataFrame(self.all_news)
            return df
        else:
            return pd.DataFrame()
    
    def _check_available_filters(self):
        """Check what filters are actually available"""
        try:
            print("Fetching available filters from API...")
            
            # Get all filters
            response = self.filters_api.get_street_account_filters(
                structured=True,
                flattened=True
            )
            
            if response and hasattr(response, 'data'):
                data = response.data
                
                # Check flattened filters
                if hasattr(data, 'flattened_filters'):
                    flattened = data.flattened_filters
                    
                    # Sectors
                    if hasattr(flattened, 'sectors') and flattened.sectors:
                        sectors = [s.name for s in flattened.sectors if hasattr(s, 'name')]
                        print(f"\n  Available Sectors ({len(sectors)}):")
                        for s in sectors[:10]:
                            print(f"    - {s}")
                        
                        # Check if Banking/Financial exists
                        banking_related = [s for s in sectors if 'bank' in s.lower() or 'financ' in s.lower()]
                        if banking_related:
                            print(f"\n  Banking-related sectors found:")
                            for s in banking_related:
                                print(f"    ✓ {s}")
                    
                    # Regions
                    if hasattr(flattened, 'regions') and flattened.regions:
                        regions = [r.name for r in flattened.regions if hasattr(r, 'name')]
                        print(f"\n  Available Regions ({len(regions)}):")
                        for r in regions:
                            print(f"    - {r}")
                        
                        # Check if Canada exists
                        canada_related = [r for r in regions if 'canad' in r.lower()]
                        if canada_related:
                            print(f"\n  Canada-related regions found:")
                            for r in canada_related:
                                print(f"    ✓ {r}")
                    
                    # Categories
                    if hasattr(flattened, 'categories') and flattened.categories:
                        categories = [c.name for c in flattened.categories if hasattr(c, 'name')]
                        print(f"\n  Available Categories ({len(categories)}):")
                        for c in categories[:10]:
                            print(f"    - {c}")
                
        except Exception as e:
            print(f"  Error fetching filters: {e}")
            self.debug_log.append(f"Filter fetch error: {str(e)}\n{traceback.format_exc()}")
    
    def _rate_limit(self):
        """Apply rate limiting"""
        time.sleep(self.request_delay)
        self.request_count += 1
        if self.request_count % 5 == 0:
            print(f"  [Rate limiting: Pausing 5 seconds after {self.request_count} requests...]")
            time.sleep(5)
    
    def _test_search(self, search_type, tickers=None, sectors=None, regions=None, 
                    categories=None, description="Search") -> bool:
        """Test a search configuration with detailed logging"""
        
        print(f"\nTesting: {description}")
        
        try:
            # Build request data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=3)
            
            request_data_dict = {
                "search_time": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                }
            }
            
            # Build the request data
            request_data = HeadlinesRequestData(
                search_time=HeadlinesRequestDataSearchTime(
                    start=start_date,
                    end=end_date
                )
            )
            
            # Add filters
            if tickers:
                request_data.tickers = [
                    HeadlinesRequestTickersObject(value=t, type="Equity")
                    for t in tickers
                ]
                request_data_dict["tickers"] = [{"value": t, "type": "Equity"} for t in tickers]
            
            if sectors:
                request_data.sectors = sectors
                request_data_dict["sectors"] = sectors
            
            if regions:
                request_data.regions = regions
                request_data_dict["regions"] = regions
            
            if categories:
                request_data.categories = categories
                request_data_dict["categories"] = categories
            
            # Log the request
            print(f"  Request details:")
            print(f"    - Type: {search_type}")
            if tickers:
                print(f"    - Tickers: {tickers}")
            if sectors:
                print(f"    - Sectors: {sectors}")
            if regions:
                print(f"    - Regions: {regions}")
            if categories:
                print(f"    - Categories: {categories}")
            
            # Create request
            request = HeadlinesRequest(
                data=request_data,
                meta=HeadlinesRequestMeta(
                    pagination=HeadlinesRequestMetaPagination(limit=10, offset=0),
                    attributes=["headlines", "storyTime", "id"]
                )
            )
            
            # Log full request for debugging
            if self.debug:
                self.debug_log.append(f"Request for {description}:\n{json.dumps(request_data_dict, indent=2)}")
            
            # Make API call
            response = self.headlines_api.get_street_account_headlines(
                headlines_request=request
            )
            
            # Check response
            if response and hasattr(response, 'data'):
                count = len(response.data) if response.data else 0
                print(f"  ✓ SUCCESS: {count} results")
                
                if count > 0:
                    # Store results
                    for item in response.data:
                        news_item = item.to_dict() if hasattr(item, 'to_dict') else {}
                        news_item['search_method'] = description
                        self.all_news.append(news_item)
                    
                    # Show sample headline
                    if hasattr(response.data[0], 'headlines'):
                        print(f"    Sample: {response.data[0].headlines[:80]}...")
                
                return True
            else:
                print(f"  ✓ SUCCESS but no data")
                return True
                
        except Exception as e:
            error_str = str(e)
            print(f"  ✗ FAILED: {error_str[:200]}")
            
            # Parse error for more details
            if "400" in error_str or "bad request" in error_str.lower():
                print(f"  ⚠️  Bad Request Details:")
                
                # Try to extract error message
                if "Invalid value" in error_str:
                    print(f"    - Invalid value error detected")
                    # Extract the invalid field/value
                    import re
                    invalid_match = re.search(r"Invalid value for `(\w+)` \((.*?)\)", error_str)
                    if invalid_match:
                        field = invalid_match.group(1)
                        value = invalid_match.group(2)
                        print(f"    - Field: {field}")
                        print(f"    - Value: {value}")
                        print(f"    - This means the API doesn't recognize this {field} value")
                
                if "must be one of" in error_str:
                    # Extract valid values
                    valid_match = re.search(r"must be one of \[(.*?)\]", error_str)
                    if valid_match:
                        valid_values = valid_match.group(1)
                        print(f"    - Valid values: {valid_values}")
                
                # Log full error
                self.debug_log.append(f"Error for {description}:\n{error_str}\n\nTraceback:\n{traceback.format_exc()}")
            
            elif "403" in error_str:
                print(f"  ⚠️  403 Forbidden - Check credentials/permissions")
            elif "429" in error_str:
                print(f"  ⚠️  429 Rate Limited - Wait before retrying")
            
            return False


def setup_factset_api_client():
    """Set up API client"""
    api_username = os.getenv("API_USERNAME")
    api_password = os.getenv("API_PASSWORD")
    
    if not api_username or not api_password:
        raise ValueError("API_USERNAME and API_PASSWORD must be set in .env file")
    
    configuration = fds.sdk.StreetAccountNews.Configuration(
        username=api_username,
        password=api_password
    )
    
    configuration.get_basic_auth_token()
    return configuration


def main():
    """Main function with debug logging"""
    
    print("\n" + "="*60)
    print("CANADIAN BANKING NEWS - DEBUG MODE")
    print("="*60)
    
    try:
        # Set up API client
        print("\nSetting up API client...")
        api_configuration = setup_factset_api_client()
        print("✓ API client configured")
        
        with fds.sdk.StreetAccountNews.ApiClient(api_configuration) as api_client:
            
            collector = CanadianBankingNewsCollector(api_client)
            
            # Collect news
            df = collector.collect_all_news(days_back=3)
            
            # Save results
            if not df.empty:
                output_dir = Path("output")
                output_dir.mkdir(exist_ok=True)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = output_dir / f"debug_news_{timestamp}.csv"
                df.to_csv(output_file, index=False)
                
                print(f"\n✅ Results saved to {output_file}")
            else:
                print("\n⚠️ No news collected")
            
            # Save debug log
            if collector.debug_log:
                debug_file = Path("output") / f"debug_log_{timestamp}.txt"
                with open(debug_file, 'w') as f:
                    for entry in collector.debug_log:
                        f.write(entry + "\n\n")
                print(f"📝 Debug log saved to {debug_file}")
    
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
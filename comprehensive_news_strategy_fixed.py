"""
Comprehensive Canadian Banking News Strategy - WITH PROPER AUTH
Includes SSL certificate and proxy configuration from working script
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
import io

import yaml
import fds.sdk.StreetAccountNews
from fds.sdk.StreetAccountNews.api import headlines_api, filters_api
from fds.sdk.StreetAccountNews.models import *
from smb.SMBConnection import SMBConnection
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class CanadianBankingNewsCollector:
    """Comprehensive news collector for Canadian banking sector"""
    
    def __init__(self, api_client):
        self.api_client = api_client
        self.headlines_api = headlines_api.HeadlinesApi(api_client)
        self.filters_api = filters_api.FiltersApi(api_client)
        
        # Canadian bank tickers - multiple formats
        self.canadian_banks = {
            "RY-CA": {"name": "Royal Bank of Canada", "aliases": ["RY", "RY.TO"]},
            "TD-CA": {"name": "Toronto-Dominion Bank", "aliases": ["TD", "TD.TO"]},
            "BMO-CA": {"name": "Bank of Montreal", "aliases": ["BMO", "BMO.TO"]},
            "BNS-CA": {"name": "Bank of Nova Scotia", "aliases": ["BNS", "BNS.TO"]},
            "CM-CA": {"name": "CIBC", "aliases": ["CM", "CM.TO"]},
            "NA-CA": {"name": "National Bank", "aliases": ["NA", "NA.TO"]},
            "LB-CA": {"name": "Laurentian Bank", "aliases": ["LB", "LB.TO"]}
        }
        
        self.all_news = []
        self.unique_stories = set()
        
        # Rate limiting
        self.request_delay = 2.0  # seconds between requests
        self.request_count = 0
        self.max_requests_per_batch = 10  # pause after this many requests
    
    def collect_all_news(self, days_back: int = 7) -> pd.DataFrame:
        """Collect news using multiple strategies with rate limiting"""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        print(f"\nCollecting news from {start_date.date()} to {end_date.date()}")
        print("="*60)
        
        # Strategy 1: Direct ticker search for each bank
        print("\n📊 STRATEGY 1: Direct ticker searches")
        print("-"*40)
        self._search_by_tickers(start_date, end_date)
        
        # Strategy 2: Search by banking sector (if not too many requests)
        if self.request_count < 20:
            print("\n🏦 STRATEGY 2: Banking sector news")
            print("-"*40)
            self._search_by_sector(start_date, end_date)
        
        # Strategy 3: Search by Canadian region (if not too many requests)
        if self.request_count < 30:
            print("\n🇨🇦 STRATEGY 3: Canadian financial news")
            print("-"*40)
            self._search_by_region(start_date, end_date)
        
        # Convert to DataFrame and deduplicate
        df = self._process_results()
        
        return df
    
    def _rate_limit(self):
        """Apply rate limiting between requests"""
        time.sleep(self.request_delay)
        self.request_count += 1
        
        # Longer pause after batch of requests
        if self.request_count % self.max_requests_per_batch == 0:
            print(f"  [Rate limiting: Pausing 10 seconds after {self.request_count} requests...]")
            time.sleep(10)
    
    def _search_by_tickers(self, start_date, end_date):
        """Search for each bank ticker"""
        
        for main_ticker, info in self.canadian_banks.items():
            # Rate limiting
            self._rate_limit()
            
            # Try main ticker only
            count = self._execute_search(
                tickers=[{"value": main_ticker, "type": "Equity"}],
                start_date=start_date,
                end_date=end_date,
                description=f"{main_ticker} ({info['name']})"
            )
    
    def _search_by_sector(self, start_date, end_date):
        """Search banking sector without ticker restrictions"""
        
        # Rate limiting
        self._rate_limit()
        
        # Just try Banking sector
        self._execute_search(
            sectors=["Banking"],
            start_date=start_date,
            end_date=end_date,
            description=f"Banking sector (all)"
        )
    
    def _search_by_region(self, start_date, end_date):
        """Search Canadian region financial news"""
        
        # Rate limiting
        self._rate_limit()
        
        # Canada region with financial sector
        self._execute_search(
            regions=["Canada"],
            sectors=["Financial"],
            start_date=start_date,
            end_date=end_date,
            description="Canada + Financial sector"
        )
    
    def _execute_search(self, tickers=None, sectors=None, regions=None, 
                       categories=None, topics=None, start_date=None, 
                       end_date=None, description="Search") -> int:
        """Execute a search and store results"""
        
        try:
            # Build request data
            request_data = HeadlinesRequestData(
                search_time=HeadlinesRequestDataSearchTime(
                    start=start_date,
                    end=end_date
                )
            )
            
            # Add filters if provided
            if tickers:
                request_data.tickers = [
                    HeadlinesRequestTickersObject(value=t["value"], type=t["type"])
                    for t in tickers
                ]
            if sectors:
                request_data.sectors = sectors
            if regions:
                request_data.regions = regions
            if categories:
                request_data.categories = categories
            if topics:
                request_data.topics = topics
            
            request = HeadlinesRequest(
                data=request_data,
                meta=HeadlinesRequestMeta(
                    pagination=HeadlinesRequestMetaPagination(limit=50, offset=0),  # Reduced limit
                    attributes=["headlines", "storyTime", "id", "primarySymbols", 
                               "symbols", "subjects", "storyBody", "url"]
                )
            )
            
            response = self.headlines_api.get_street_account_headlines(
                headlines_request=request
            )
            
            count = 0
            new_count = 0
            
            if response and response.data:
                for item in response.data:
                    # Create unique ID
                    story_id = getattr(item, 'id', None)
                    
                    if story_id and story_id not in self.unique_stories:
                        self.unique_stories.add(story_id)
                        
                        # Convert to dict and add metadata
                        news_item = item.to_dict() if hasattr(item, 'to_dict') else {}
                        news_item['search_method'] = description
                        news_item['retrieved_at'] = datetime.now().isoformat()
                        
                        # Check Canadian bank relevance
                        news_item['canadian_banks_mentioned'] = self._check_bank_mentions(news_item)
                        
                        self.all_news.append(news_item)
                        new_count += 1
                
                count = len(response.data)
            
            print(f"{description}: {count} results ({new_count} new)")
            return count
            
        except Exception as e:
            print(f"{description}: Error - {str(e)[:100]}")
            return 0
    
    def _check_bank_mentions(self, news_item: Dict) -> List[str]:
        """Check which Canadian banks are mentioned in the news"""
        
        mentioned = []
        
        # Check symbols
        symbols = news_item.get('symbols', []) or []
        primary_symbols = news_item.get('primarySymbols', []) or []
        all_symbols = set(symbols + primary_symbols)
        
        for ticker, info in self.canadian_banks.items():
            # Check main ticker and aliases
            check_tickers = [ticker] + info['aliases']
            if any(t in all_symbols for t in check_tickers):
                mentioned.append(ticker)
                continue
            
            # Check in headlines and story body
            headline = news_item.get('headlines', '')
            story = news_item.get('storyBody', '')
            full_text = f"{headline} {story}".lower()
            
            # Check bank name
            if info['name'].lower() in full_text:
                mentioned.append(ticker)
        
        return mentioned
    
    def _process_results(self) -> pd.DataFrame:
        """Process and deduplicate results"""
        
        if not self.all_news:
            print("\nNo news found!")
            return pd.DataFrame()
        
        df = pd.DataFrame(self.all_news)
        
        # Add analysis columns
        df['is_canadian_banking'] = df['canadian_banks_mentioned'].apply(lambda x: len(x) > 0)
        df['num_banks_mentioned'] = df['canadian_banks_mentioned'].apply(len)
        
        # Sort by date
        if 'storyTime' in df.columns:
            df['storyTime'] = pd.to_datetime(df['storyTime'], errors='coerce')
            df = df.sort_values('storyTime', ascending=False)
        
        print(f"\n📊 RESULTS SUMMARY")
        print("="*60)
        print(f"Total unique stories: {len(df)}")
        print(f"Stories mentioning Canadian banks: {df['is_canadian_banking'].sum()}")
        
        return df


def setup_ssl_certificate(nas_conn=None, local_cert_path=None):
    """Set up SSL certificate for API use"""
    
    if local_cert_path and os.path.exists(local_cert_path):
        # Use local certificate file
        print(f"Using local SSL certificate: {local_cert_path}")
        return local_cert_path
    
    # If no local cert, try to create a temp one or skip
    print("Warning: No SSL certificate configured")
    return None


def setup_proxy_configuration():
    """Configure proxy URL for API authentication"""
    
    proxy_user = os.getenv("PROXY_USER")
    proxy_password = os.getenv("PROXY_PASSWORD")
    proxy_url = os.getenv("PROXY_URL")
    
    if not all([proxy_user, proxy_password, proxy_url]):
        print("Warning: Proxy settings not fully configured")
        return None
    
    proxy_domain = os.getenv("PROXY_DOMAIN", "MAPLE")
    
    # Escape domain and user for NTLM authentication
    escaped_domain = quote(proxy_domain + "\\" + proxy_user)
    quoted_password = quote(proxy_password)
    
    # Construct proxy URL
    proxy_url_formatted = f"http://{escaped_domain}:{quoted_password}@{proxy_url}"
    
    print("Proxy configuration completed")
    return proxy_url_formatted


def setup_factset_api_client(proxy_url=None, ssl_cert_path=None):
    """Configure FactSet API client with optional proxy and SSL settings"""
    
    api_username = os.getenv("API_USERNAME")
    api_password = os.getenv("API_PASSWORD")
    
    if not api_username or not api_password:
        raise ValueError("API_USERNAME and API_PASSWORD must be set in .env file")
    
    # Create configuration
    config_params = {
        "username": api_username,
        "password": api_password,
    }
    
    # Add optional parameters
    if proxy_url:
        config_params["proxy"] = proxy_url
    
    if ssl_cert_path:
        config_params["ssl_ca_cert"] = ssl_cert_path
    
    configuration = fds.sdk.StreetAccountNews.Configuration(**config_params)
    
    # Generate authentication token
    configuration.get_basic_auth_token()
    
    print("FactSet API client configured successfully")
    return configuration


def main():
    """Main function with proper authentication setup"""
    
    print("\n" + "="*60)
    print("COMPREHENSIVE CANADIAN BANKING NEWS COLLECTOR")
    print("WITH AUTHENTICATION")
    print("="*60)
    
    ssl_cert_path = None
    
    try:
        # Step 1: Check environment variables
        print("\n1. Checking environment variables...")
        if not os.getenv("API_USERNAME") or not os.getenv("API_PASSWORD"):
            print("ERROR: API_USERNAME and API_PASSWORD must be set in .env file")
            return
        print("   ✓ API credentials found")
        
        # Step 2: Set up SSL certificate (optional)
        print("\n2. Setting up SSL certificate...")
        ssl_cert_path = setup_ssl_certificate()
        if ssl_cert_path:
            os.environ["REQUESTS_CA_BUNDLE"] = ssl_cert_path
            os.environ["SSL_CERT_FILE"] = ssl_cert_path
        
        # Step 3: Configure proxy (optional)
        print("\n3. Configuring proxy...")
        proxy_url = setup_proxy_configuration()
        
        # Step 4: Set up FactSet API client
        print("\n4. Setting up FactSet API client...")
        api_configuration = setup_factset_api_client(proxy_url, ssl_cert_path)
        
        print("\n5. Starting news collection...")
        print("-"*60)
        
        with fds.sdk.StreetAccountNews.ApiClient(api_configuration) as api_client:
            
            collector = CanadianBankingNewsCollector(api_client)
            
            # Collect news with shorter timeframe to reduce requests
            df = collector.collect_all_news(days_back=3)  # Only last 3 days
            
            # Save results
            if not df.empty:
                output_dir = Path("output")
                output_dir.mkdir(exist_ok=True)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Save results
                output_file = output_dir / f"comprehensive_news_{timestamp}.csv"
                df.to_csv(output_file, index=False)
                
                print(f"\n✅ Results saved to {output_file}")
                
                # Show sample headlines
                if 'headlines' in df.columns:
                    print("\n📰 SAMPLE HEADLINES:")
                    for idx, row in df.head(5).iterrows():
                        headline = row.get('headlines', 'No headline')[:100]
                        print(f"  • {headline}...")
            else:
                print("\n⚠️ No news found")
    
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        if ssl_cert_path and ssl_cert_path.startswith(tempfile.gettempdir()):
            try:
                os.unlink(ssl_cert_path)
                print("\n✓ Cleaned up temporary files")
            except:
                pass


if __name__ == "__main__":
    main()
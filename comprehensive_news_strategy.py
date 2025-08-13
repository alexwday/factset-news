"""
Comprehensive Canadian Banking News Strategy
Combines multiple approaches to capture ALL relevant news
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Set
import pandas as pd
from pathlib import Path

import yaml
import fds.sdk.StreetAccountNews
from fds.sdk.StreetAccountNews.api import headlines_api, filters_api
from fds.sdk.StreetAccountNews.models import *
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
            "RY-CA": {"name": "Royal Bank of Canada", "aliases": ["RY", "RY.TO", "RY:CN"]},
            "TD-CA": {"name": "Toronto-Dominion Bank", "aliases": ["TD", "TD.TO", "TD:CN"]},
            "BMO-CA": {"name": "Bank of Montreal", "aliases": ["BMO", "BMO.TO", "BMO:CN"]},
            "BNS-CA": {"name": "Bank of Nova Scotia", "aliases": ["BNS", "BNS.TO", "BNS:CN"]},
            "CM-CA": {"name": "CIBC", "aliases": ["CM", "CM.TO", "CM:CN"]},
            "NA-CA": {"name": "National Bank", "aliases": ["NA", "NA.TO", "NA:CN"]},
            "LB-CA": {"name": "Laurentian Bank", "aliases": ["LB", "LB.TO", "LB:CN"]}
        }
        
        # Banking-related categories and topics to search
        self.banking_keywords = [
            "Banking", "Banks", "Financial", "Credit", "Lending", "Deposits",
            "Interest Rates", "Mortgages", "Capital Markets", "Wealth Management",
            "Earnings", "Dividends", "Regulatory", "Basel", "OSFI"
        ]
        
        self.all_news = []
        self.unique_stories = set()
    
    def collect_all_news(self, days_back: int = 30) -> pd.DataFrame:
        """Collect news using multiple strategies"""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        print(f"\nCollecting news from {start_date.date()} to {end_date.date()}")
        print("="*60)
        
        # Strategy 1: Direct ticker search for each bank
        print("\n📊 STRATEGY 1: Direct ticker searches")
        print("-"*40)
        self._search_by_tickers(start_date, end_date)
        
        # Strategy 2: Search by banking sector
        print("\n🏦 STRATEGY 2: Banking sector news")
        print("-"*40)
        self._search_by_sector(start_date, end_date)
        
        # Strategy 3: Search by Canadian region
        print("\n🇨🇦 STRATEGY 3: Canadian financial news")
        print("-"*40)
        self._search_by_region(start_date, end_date)
        
        # Strategy 4: Search by financial categories
        print("\n📈 STRATEGY 4: Financial categories")
        print("-"*40)
        self._search_by_categories(start_date, end_date)
        
        # Strategy 5: Search combinations
        print("\n🔄 STRATEGY 5: Combined filters")
        print("-"*40)
        self._search_combined(start_date, end_date)
        
        # Convert to DataFrame and deduplicate
        df = self._process_results()
        
        return df
    
    def _search_by_tickers(self, start_date, end_date):
        """Search for each bank ticker and its aliases"""
        
        for main_ticker, info in self.canadian_banks.items():
            # Try main ticker
            count = self._execute_search(
                tickers=[{"value": main_ticker, "type": "Equity"}],
                start_date=start_date,
                end_date=end_date,
                description=f"{main_ticker} ({info['name']})"
            )
            
            # Try aliases if main ticker has limited results
            if count < 10:
                for alias in info['aliases']:
                    self._execute_search(
                        tickers=[{"value": alias, "type": "Equity"}],
                        start_date=start_date,
                        end_date=end_date,
                        description=f"  Alias: {alias}"
                    )
    
    def _search_by_sector(self, start_date, end_date):
        """Search banking sector without ticker restrictions"""
        
        # Try different sector variations
        sectors_to_try = ["Banking", "Financial", "Banks", "Financials"]
        
        for sector in sectors_to_try:
            self._execute_search(
                sectors=[sector],
                start_date=start_date,
                end_date=end_date,
                description=f"Sector: {sector}"
            )
    
    def _search_by_region(self, start_date, end_date):
        """Search Canadian region financial news"""
        
        # Canada region with financial sector
        self._execute_search(
            regions=["Canada"],
            sectors=["Financial"],
            start_date=start_date,
            end_date=end_date,
            description="Canada + Financial sector"
        )
        
        # North America banking
        self._execute_search(
            regions=["North America"],
            sectors=["Banking"],
            start_date=start_date,
            end_date=end_date,
            description="North America + Banking"
        )
    
    def _search_by_categories(self, start_date, end_date):
        """Search by news categories relevant to banking"""
        
        # Get available categories first
        try:
            response = self.filters_api.get_street_account_filters_categories()
            if response and hasattr(response, 'data'):
                categories = [cat.name for cat in response.data.categories 
                             if hasattr(cat, 'name')]
                
                # Filter for banking-related categories
                banking_categories = [cat for cat in categories 
                                     if any(keyword.lower() in cat.lower() 
                                           for keyword in ["bank", "financial", "credit", 
                                                         "earnings", "regulatory"])]
                
                for category in banking_categories[:5]:  # Top 5 relevant
                    self._execute_search(
                        categories=[category],
                        regions=["Canada"],
                        start_date=start_date,
                        end_date=end_date,
                        description=f"Category: {category}"
                    )
        except Exception as e:
            print(f"Could not fetch categories: {e}")
    
    def _search_combined(self, start_date, end_date):
        """Combined search strategies"""
        
        # All Canadian bank tickers together
        all_tickers = []
        for ticker in self.canadian_banks.keys():
            all_tickers.append({"value": ticker, "type": "Equity"})
        
        self._execute_search(
            tickers=all_tickers,
            start_date=start_date,
            end_date=end_date,
            description="All Canadian banks combined"
        )
        
        # Canadian banking sector without ticker restriction
        self._execute_search(
            regions=["Canada"],
            sectors=["Banking"],
            categories=["Earnings"],
            start_date=start_date,
            end_date=end_date,
            description="Canada + Banking + Earnings"
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
                    pagination=HeadlinesRequestMetaPagination(limit=100, offset=0),
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
        print(f"Average banks mentioned per story: {df['num_banks_mentioned'].mean():.2f}")
        
        # Bank mention frequency
        print("\n🏦 BANK MENTION FREQUENCY:")
        bank_counts = {}
        for banks in df['canadian_banks_mentioned']:
            for bank in banks:
                bank_counts[bank] = bank_counts.get(bank, 0) + 1
        
        for ticker, count in sorted(bank_counts.items(), key=lambda x: x[1], reverse=True):
            bank_name = self.canadian_banks[ticker]['name']
            print(f"  {ticker} ({bank_name}): {count} stories")
        
        # Top headlines
        print("\n📰 SAMPLE HEADLINES (Canadian banking related):")
        canadian_df = df[df['is_canadian_banking'] == True]
        for idx, row in canadian_df.head(10).iterrows():
            headline = row.get('headlines', 'No headline')[:100]
            banks = ', '.join(row['canadian_banks_mentioned'])
            print(f"  • [{banks}] {headline}...")
        
        return df


def main():
    """Main function"""
    
    print("\n" + "="*60)
    print("COMPREHENSIVE CANADIAN BANKING NEWS COLLECTOR")
    print("="*60)
    
    # Setup API
    api_username = os.getenv("API_USERNAME", "test_user")
    api_password = os.getenv("API_PASSWORD", "test_pass")
    
    configuration = fds.sdk.StreetAccountNews.Configuration(
        username=api_username,
        password=api_password,
    )
    
    # CRITICAL: Generate the auth token!
    configuration.get_basic_auth_token()
    
    with fds.sdk.StreetAccountNews.ApiClient(configuration) as api_client:
        
        collector = CanadianBankingNewsCollector(api_client)
        
        # Collect news
        df = collector.collect_all_news(days_back=7)  # Last 7 days for testing
        
        # Save results
        if not df.empty:
            output_dir = Path("output")
            output_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save full dataset
            df.to_csv(output_dir / f"comprehensive_news_{timestamp}.csv", index=False)
            
            # Save Canadian banking specific
            canadian_df = df[df['is_canadian_banking'] == True]
            canadian_df.to_csv(output_dir / f"canadian_banking_news_{timestamp}.csv", index=False)
            
            # Save summary
            with open(output_dir / f"news_summary_{timestamp}.txt", 'w') as f:
                f.write("CANADIAN BANKING NEWS SUMMARY\n")
                f.write("="*60 + "\n\n")
                f.write(f"Total stories collected: {len(df)}\n")
                f.write(f"Canadian banking stories: {len(canadian_df)}\n")
                f.write(f"Date range: {df['storyTime'].min()} to {df['storyTime'].max()}\n\n")
                
                f.write("Search methods used:\n")
                for method in df['search_method'].unique():
                    count = len(df[df['search_method'] == method])
                    f.write(f"  - {method}: {count} stories\n")
            
            print(f"\n✅ Results saved to output/")
            print(f"  - comprehensive_news_{timestamp}.csv")
            print(f"  - canadian_banking_news_{timestamp}.csv")
            print(f"  - news_summary_{timestamp}.txt")


if __name__ == "__main__":
    main()
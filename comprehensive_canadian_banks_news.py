"""
Comprehensive Canadian Banks News Collection
Combines multiple search strategies to get ALL news about Canadian banks
Includes proper authentication with proxy and SSL
"""

import os
import tempfile
import json
import time
from datetime import datetime, timedelta
from urllib.parse import quote
from typing import Dict, Any, List, Set, Optional
import pandas as pd
from pathlib import Path
import io
import logging

import yaml
import fds.sdk.StreetAccountNews
from fds.sdk.StreetAccountNews.api import headlines_api, filters_api
from fds.sdk.StreetAccountNews.models import *
from smb.SMBConnection import SMBConnection
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Global variables
logger = None


def setup_logging() -> logging.Logger:
    """Set up console logging"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


def get_nas_connection() -> Optional[SMBConnection]:
    """Create and return an SMB connection to the NAS"""
    try:
        conn = SMBConnection(
            username=os.getenv("NAS_USERNAME"),
            password=os.getenv("NAS_PASSWORD"),
            my_name=os.getenv("CLIENT_MACHINE_NAME", "SYNC-CLIENT"),
            remote_name=os.getenv("NAS_SERVER_NAME"),
            use_ntlm_v2=True,
            is_direct_tcp=True,
        )
        
        nas_port = int(os.getenv("NAS_PORT", 445))
        if conn.connect(os.getenv("NAS_SERVER_IP"), nas_port):
            logger.info("NAS connection established")
            return conn
        else:
            logger.error("Failed to connect to NAS")
            return None
            
    except Exception as e:
        logger.error(f"Error connecting to NAS: {e}")
        return None


def setup_ssl_certificate(nas_conn: SMBConnection) -> Optional[str]:
    """Download SSL certificate from NAS and set up for API use"""
    try:
        cert_path = "Finance Data and Analytics/DSA/Earnings Call Transcripts/Inputs/Certificate/rbc-ca-bundle.cer"
        logger.info("Downloading SSL certificate from NAS...")
        
        file_obj = io.BytesIO()
        nas_conn.retrieveFile(os.getenv("NAS_SHARE_NAME"), cert_path, file_obj)
        file_obj.seek(0)
        cert_data = file_obj.read()
        
        # Create temporary certificate file
        temp_cert = tempfile.NamedTemporaryFile(mode="wb", suffix=".cer", delete=False)
        temp_cert.write(cert_data)
        temp_cert.close()
        
        # Set environment variables for SSL
        os.environ["REQUESTS_CA_BUNDLE"] = temp_cert.name
        os.environ["SSL_CERT_FILE"] = temp_cert.name
        
        logger.info("SSL certificate configured successfully")
        return temp_cert.name
        
    except Exception as e:
        logger.error(f"Error setting up SSL certificate: {e}")
        return None


def setup_proxy_configuration() -> str:
    """Configure proxy URL for API authentication"""
    try:
        proxy_user = os.getenv("PROXY_USER")
        proxy_password = os.getenv("PROXY_PASSWORD")
        proxy_url = os.getenv("PROXY_URL")
        proxy_domain = os.getenv("PROXY_DOMAIN", "MAPLE")
        
        # Escape domain and user for NTLM authentication
        escaped_domain = quote(proxy_domain + "\\" + proxy_user)
        quoted_password = quote(proxy_password)
        
        # Construct proxy URL
        proxy_url_formatted = f"http://{escaped_domain}:{quoted_password}@{proxy_url}"
        
        logger.info("Proxy configuration completed")
        return proxy_url_formatted
        
    except Exception as e:
        logger.error(f"Error configuring proxy: {e}")
        raise


def setup_factset_api_client(proxy_url: str, ssl_cert_path: str):
    """Configure FactSet API client with proxy and SSL settings"""
    try:
        api_username = os.getenv("API_USERNAME")
        api_password = os.getenv("API_PASSWORD")
        
        # Configure FactSet Street Account News API client
        configuration = fds.sdk.StreetAccountNews.Configuration(
            username=api_username,
            password=api_password,
            proxy=proxy_url,
            ssl_ca_cert=ssl_cert_path,
        )
        
        # Generate authentication token
        configuration.get_basic_auth_token()
        
        logger.info("FactSet API client configured successfully")
        return configuration
        
    except Exception as e:
        logger.error(f"Error setting up FactSet API client: {e}")
        raise


class ComprehensiveNewsCollector:
    """Comprehensive news collector for Canadian banks"""
    
    def __init__(self, api_client):
        self.api_client = api_client
        self.headlines_api = headlines_api.HeadlinesApi(api_client)
        self.filters_api = filters_api.FiltersApi(api_client)
        
        # Canadian bank tickers
        self.canadian_banks = {
            "RY-CA": "Royal Bank of Canada",
            "TD-CA": "Toronto-Dominion Bank",
            "BMO-CA": "Bank of Montreal",
            "BNS-CA": "Bank of Nova Scotia",
            "CM-CA": "CIBC",
            "NA-CA": "National Bank of Canada",
            "LB-CA": "Laurentian Bank",
        }
        
        self.all_news = []
        self.unique_stories = set()
        self.request_count = 0
        self.request_delay = 2.0  # seconds between requests
    
    def collect_news(self, days_back: int = 30) -> pd.DataFrame:
        """Collect news using multiple search strategies"""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"Collecting news from {start_date.date()} to {end_date.date()}")
        logger.info("="*60)
        
        # Strategy 1: Direct ticker search for each bank
        logger.info("\n📊 STRATEGY 1: Direct ticker searches")
        for ticker, name in self.canadian_banks.items():
            self._rate_limit()
            count = self._search_news(
                tickers=[ticker],
                start_date=start_date,
                end_date=end_date,
                description=f"{ticker} ({name})"
            )
            logger.info(f"  {ticker}: {count} headlines")
        
        # Strategy 2: Try different sector values since "Banking" gives 400 error
        logger.info("\n🏦 STRATEGY 2: Financial sector news")
        self._rate_limit()
        count = self._search_news(
            sectors=["Financial"],  # Changed from "Banking" which causes 400 error
            start_date=start_date,
            end_date=end_date,
            description="Financial sector"
        )
        logger.info(f"  Financial sector: {count} headlines")
        
        # Strategy 3: Financial sector with Canada region
        logger.info("\n🇨🇦 STRATEGY 3: Canadian financial news")
        self._rate_limit()
        count = self._search_news(
            regions=["North America"],
            sectors=["Financial"],
            start_date=start_date,
            end_date=end_date,
            description="North America Financial"
        )
        logger.info(f"  North America Financial: {count} headlines")
        
        # Process results
        return self._process_results()
    
    def _rate_limit(self):
        """Apply rate limiting between requests"""
        time.sleep(self.request_delay)
        self.request_count += 1
        
        if self.request_count % 10 == 0:
            logger.info(f"  [Rate limiting: Pausing 10 seconds after {self.request_count} requests...]")
            time.sleep(10)
    
    def _search_news(self, tickers=None, sectors=None, regions=None, 
                    start_date=None, end_date=None, description="Search") -> int:
        """Execute a search and store results"""
        
        try:
            # Build request
            request_data = HeadlinesRequestData(
                search_time=HeadlinesRequestDataSearchTime(
                    start=start_date,
                    end=end_date
                )
            )
            
            # Add filters if provided
            if tickers:
                request_data.tickers = [
                    HeadlinesRequestTickersObject(value=t, type="Equity")
                    for t in tickers
                ]
            if sectors:
                request_data.sectors = sectors
            if regions:
                request_data.regions = regions
            
            request = HeadlinesRequest(
                data=request_data,
                meta=HeadlinesRequestMeta(
                    pagination=HeadlinesRequestMetaPagination(limit=100, offset=0),
                    attributes=["headlines", "storyTime", "id", "primarySymbols", 
                               "symbols", "subjects", "storyBody", "url"]
                )
            )
            
            # Make API call
            response = self.headlines_api.get_street_account_headlines(
                headlines_request=request
            )
            
            count = 0
            new_count = 0
            
            if response and response.data:
                for item in response.data:
                    story_id = getattr(item, 'id', None)
                    
                    if story_id and story_id not in self.unique_stories:
                        self.unique_stories.add(story_id)
                        
                        # Convert to dict and add metadata
                        news_item = item.to_dict() if hasattr(item, 'to_dict') else {}
                        news_item['search_method'] = description
                        news_item['retrieved_at'] = datetime.now().isoformat()
                        
                        # Check for Canadian bank mentions
                        news_item['canadian_banks_mentioned'] = self._check_bank_mentions(news_item)
                        
                        self.all_news.append(news_item)
                        new_count += 1
                
                count = len(response.data)
            
            return new_count
            
        except Exception as e:
            error_msg = str(e)
            if "400" in error_msg:
                logger.warning(f"{description}: Bad Request (400) - Invalid filter value")
            else:
                logger.error(f"{description}: Error - {error_msg[:100]}")
            return 0
    
    def _check_bank_mentions(self, news_item: Dict) -> List[str]:
        """Check which Canadian banks are mentioned"""
        
        mentioned = []
        
        # Check symbols
        symbols = news_item.get('symbols', []) or []
        primary_symbols = news_item.get('primarySymbols', []) or []
        all_symbols = set(symbols + primary_symbols)
        
        for ticker in self.canadian_banks.keys():
            if ticker in all_symbols or ticker.replace("-CA", "") in all_symbols:
                mentioned.append(ticker)
                continue
            
            # Check in text
            headline = news_item.get('headlines', '')
            story = news_item.get('storyBody', '')
            full_text = f"{headline} {story}".lower()
            
            bank_name = self.canadian_banks[ticker].lower()
            if bank_name in full_text:
                mentioned.append(ticker)
        
        return mentioned
    
    def _process_results(self) -> pd.DataFrame:
        """Process and return results as DataFrame"""
        
        if not self.all_news:
            logger.warning("No news found!")
            return pd.DataFrame()
        
        df = pd.DataFrame(self.all_news)
        
        # Add analysis columns
        df['is_canadian_banking'] = df['canadian_banks_mentioned'].apply(lambda x: len(x) > 0)
        df['num_banks_mentioned'] = df['canadian_banks_mentioned'].apply(len)
        
        # Sort by date
        if 'storyTime' in df.columns:
            df['storyTime'] = pd.to_datetime(df['storyTime'], errors='coerce')
            df = df.sort_values('storyTime', ascending=False)
        
        # Summary statistics
        logger.info(f"\n📊 RESULTS SUMMARY")
        logger.info("="*60)
        logger.info(f"Total unique stories: {len(df)}")
        logger.info(f"Stories mentioning Canadian banks: {df['is_canadian_banking'].sum()}")
        
        # Bank mention frequency
        bank_counts = {}
        for banks in df['canadian_banks_mentioned']:
            for bank in banks:
                bank_counts[bank] = bank_counts.get(bank, 0) + 1
        
        logger.info("\n🏦 BANK MENTION FREQUENCY:")
        for ticker, count in sorted(bank_counts.items(), key=lambda x: x[1], reverse=True):
            bank_name = self.canadian_banks.get(ticker, ticker)
            logger.info(f"  {ticker} ({bank_name}): {count} stories")
        
        return df


def main():
    """Main function"""
    global logger
    
    # Initialize logging
    logger = setup_logging()
    
    logger.info("\n" + "="*60)
    logger.info("COMPREHENSIVE CANADIAN BANKS NEWS COLLECTION")
    logger.info("="*60)
    
    nas_conn = None
    ssl_cert_path = None
    
    try:
        # Step 1: Validate environment variables
        logger.info("\nValidating environment variables...")
        required_vars = ["API_USERNAME", "API_PASSWORD", "PROXY_USER", 
                        "PROXY_PASSWORD", "PROXY_URL", "NAS_USERNAME", 
                        "NAS_PASSWORD", "NAS_SERVER_IP", "NAS_SERVER_NAME", 
                        "NAS_SHARE_NAME"]
        
        missing = [v for v in required_vars if not os.getenv(v)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        # Step 2: Connect to NAS
        logger.info("Connecting to NAS...")
        nas_conn = get_nas_connection()
        if not nas_conn:
            raise RuntimeError("Failed to establish NAS connection")
        
        # Step 3: Set up SSL certificate
        logger.info("Setting up SSL certificate...")
        ssl_cert_path = setup_ssl_certificate(nas_conn)
        if not ssl_cert_path:
            raise RuntimeError("Failed to set up SSL certificate")
        
        # Step 4: Configure proxy
        logger.info("Configuring proxy...")
        proxy_url = setup_proxy_configuration()
        
        # Step 5: Set up FactSet API client
        logger.info("Setting up FactSet API client...")
        api_configuration = setup_factset_api_client(proxy_url, ssl_cert_path)
        
        # Step 6: Collect news
        with fds.sdk.StreetAccountNews.ApiClient(api_configuration) as api_client:
            collector = ComprehensiveNewsCollector(api_client)
            
            # Collect news for specified period
            df = collector.collect_news(days_back=30)  # Last 30 days
            
            # Save results
            if not df.empty:
                output_dir = Path("output")
                output_dir.mkdir(exist_ok=True)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Save full dataset
                output_file = output_dir / f"comprehensive_news_{timestamp}.csv"
                df.to_csv(output_file, index=False)
                logger.info(f"\n✅ Full results saved to {output_file}")
                
                # Save Canadian banking specific
                canadian_df = df[df['is_canadian_banking'] == True]
                if not canadian_df.empty:
                    canadian_file = output_dir / f"canadian_banking_news_{timestamp}.csv"
                    canadian_df.to_csv(canadian_file, index=False)
                    logger.info(f"✅ Canadian banking news saved to {canadian_file}")
                
                # Create summary Excel with both sheets
                excel_file = output_dir / f"news_summary_{timestamp}.xlsx"
                with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                    # Summary sheet
                    summary_data = []
                    for ticker, name in collector.canadian_banks.items():
                        ticker_df = df[df['canadian_banks_mentioned'].apply(lambda x: ticker in x)]
                        
                        # Check if primarySymbols exists and handle properly
                        primary_count = 0
                        if 'primarySymbols' in df.columns:
                            primary_rows = ticker_df[ticker_df['primarySymbols'].notna()]
                            for _, row in primary_rows.iterrows():
                                symbols = row['primarySymbols']
                                if isinstance(symbols, list) and ticker in symbols:
                                    primary_count += 1
                        
                        summary_data.append({
                            'Ticker': ticker,
                            'Bank': name,
                            'Total News': len(ticker_df),
                            'As Primary': primary_count
                        })
                    
                    summary_df = pd.DataFrame(summary_data)
                    if not summary_df.empty:
                        summary_df.to_excel(writer, sheet_name='Summary', index=False)
                    
                    # Canadian banking news - only add if we have data
                    if not canadian_df.empty:
                        # Select columns that exist
                        cols_to_export = []
                        for col in ['headlines', 'storyTime', 'canadian_banks_mentioned']:
                            if col in canadian_df.columns:
                                cols_to_export.append(col)
                        
                        if cols_to_export:
                            export_df = canadian_df[cols_to_export].head(100)
                            export_df.to_excel(writer, sheet_name='Top 100 News', index=False)
                    
                    # If no sheets were added, add a default sheet
                    if len(writer.sheets) == 0:
                        pd.DataFrame({'Note': ['No Canadian banking news found']}).to_excel(
                            writer, sheet_name='No Data', index=False
                        )
                
                logger.info(f"✅ Summary Excel saved to {excel_file}")
                
                # Print sample headlines
                logger.info("\n📰 SAMPLE CANADIAN BANKING HEADLINES:")
                for idx, row in canadian_df.head(5).iterrows():
                    headline = row.get('headlines', 'No headline')[:100]
                    banks = ', '.join(row['canadian_banks_mentioned'])
                    logger.info(f"  [{banks}] {headline}...")
            else:
                logger.warning("No news collected")
    
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    
    finally:
        # Cleanup
        if nas_conn:
            nas_conn.close()
            logger.info("NAS connection closed")
        
        if ssl_cert_path and os.path.exists(ssl_cert_path):
            try:
                os.unlink(ssl_cert_path)
                logger.info("Temporary SSL certificate cleaned up")
            except:
                pass
        
        logger.info("\n=== COLLECTION COMPLETE ===")


if __name__ == "__main__":
    main()
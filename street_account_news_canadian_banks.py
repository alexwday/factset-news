"""
Street Account News for Canadian Banks
Downloads real-time news from FactSet Street Account News API for Canadian financial institutions.
Mirrors the structure and patterns from the Events and Transcripts example.
"""

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
import requests
import pandas as pd
from pathlib import Path

import yaml
import fds.sdk.StreetAccountNews
from fds.sdk.StreetAccountNews.api import headlines_api, filters_api, views_api
from fds.sdk.StreetAccountNews.models import *
from smb.SMBConnection import SMBConnection
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Global variables
config = {}
logger = None
execution_log = []  # Detailed execution log entries
error_log = []  # Error log entries (only if errors occur)


def setup_logging() -> logging.Logger:
    """Set up minimal console logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


def log_console(message: str, level: str = "INFO"):
    """Log minimal message to console."""
    global logger
    if level == "ERROR":
        logger.error(message)
    elif level == "WARNING":
        logger.warning(message)
    else:
        logger.info(message)


def log_execution(message: str, details: Dict[str, Any] = None):
    """Log detailed execution information for main log file."""
    global execution_log
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "message": message,
        "details": details or {},
    }
    execution_log.append(log_entry)


def log_error(message: str, error_type: str, details: Dict[str, Any] = None):
    """Log error information for error log file."""
    global error_log
    error_entry = {
        "timestamp": datetime.now().isoformat(),
        "error_type": error_type,
        "message": message,
        "details": details or {},
    }
    error_log.append(error_entry)


def save_logs_to_nas(nas_conn: SMBConnection, stage_summary: Dict[str, Any]):
    """Save execution and error logs to NAS at completion."""
    global execution_log, error_log

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logs_path = config["street_account_news"]["output_logs_path"]

    # Create logs directory
    nas_create_directory_recursive(nas_conn, logs_path)

    # Save main execution log
    main_log_content = {
        "stage": "street_account_news_canadian_banks",
        "execution_start": (
            execution_log[0]["timestamp"]
            if execution_log
            else datetime.now().isoformat()
        ),
        "execution_end": datetime.now().isoformat(),
        "summary": stage_summary,
        "execution_log": execution_log,
    }

    main_log_filename = f"street_account_news_canadian_banks_{timestamp}.json"
    main_log_path = nas_path_join(logs_path, main_log_filename)
    main_log_json = json.dumps(main_log_content, indent=2)
    main_log_obj = io.BytesIO(main_log_json.encode("utf-8"))

    if nas_upload_file(nas_conn, main_log_obj, main_log_path):
        log_console(f"Execution log saved: {main_log_filename}")

    # Save error log only if errors exist
    if error_log:
        errors_path = nas_path_join(logs_path, "Errors")
        nas_create_directory_recursive(nas_conn, errors_path)

        error_log_content = {
            "stage": "street_account_news_canadian_banks",
            "execution_time": datetime.now().isoformat(),
            "total_errors": len(error_log),
            "error_summary": stage_summary.get("errors", {}),
            "errors": error_log,
        }

        error_log_filename = (
            f"street_account_news_canadian_banks_errors_{timestamp}.json"
        )
        error_log_path = nas_path_join(errors_path, error_log_filename)
        error_log_json = json.dumps(error_log_content, indent=2)
        error_log_obj = io.BytesIO(error_log_json.encode("utf-8"))

        if nas_upload_file(nas_conn, error_log_obj, error_log_path):
            log_console(f"Error log saved: {error_log_filename}", "WARNING")


def validate_environment_variables() -> None:
    """Validate all required environment variables are present."""
    
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
        "NAS_BASE_PATH",
        "NAS_PORT",
        "CONFIG_PATH",
        "CLIENT_MACHINE_NAME",
    ]

    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        log_error(
            error_msg,
            "environment_validation",
            {
                "missing_variables": missing_vars,
                "total_required": len(required_env_vars),
            },
        )
        raise ValueError(error_msg)

    log_execution(
        "Environment variables validated successfully",
        {
            "total_variables": len(required_env_vars),
            "variables_checked": required_env_vars,
        },
    )


def get_nas_connection() -> Optional[SMBConnection]:
    """Create and return an SMB connection to the NAS."""
    
    try:
        conn = SMBConnection(
            username=os.getenv("NAS_USERNAME"),
            password=os.getenv("NAS_PASSWORD"),
            my_name=os.getenv("CLIENT_MACHINE_NAME"),
            remote_name=os.getenv("NAS_SERVER_NAME"),
            use_ntlm_v2=True,
            is_direct_tcp=True,
        )

        nas_port = int(os.getenv("NAS_PORT", 445))
        if conn.connect(os.getenv("NAS_SERVER_IP"), nas_port):
            log_execution(
                "NAS connection established successfully",
                {
                    "connection_type": "SMB/CIFS",
                    "port": nas_port,
                    "share_name": os.getenv("NAS_SHARE_NAME"),
                },
            )
            return conn
        else:
            log_error(
                "Failed to connect to NAS",
                "nas_connection",
                {"connection_type": "SMB/CIFS", "port": nas_port},
            )
            return None

    except Exception as e:
        log_error(
            f"Error connecting to NAS: {e}",
            "nas_connection",
            {"connection_type": "SMB/CIFS", "error_details": str(e)},
        )
        return None


def nas_download_file(conn: SMBConnection, nas_file_path: str) -> Optional[bytes]:
    """Download a file from NAS and return as bytes."""
    try:
        file_obj = io.BytesIO()
        conn.retrieveFile(os.getenv("NAS_SHARE_NAME"), nas_file_path, file_obj)
        file_obj.seek(0)
        content = file_obj.read()
        log_execution(
            f"Successfully downloaded file from NAS: {nas_file_path}",
            {"file_path": nas_file_path, "file_size": len(content)},
        )
        return content
    except Exception as e:
        log_error(
            f"Failed to download file from NAS {nas_file_path}: {e}",
            "nas_download",
            {"file_path": nas_file_path, "error_details": str(e)},
        )
        return None


def nas_upload_file(
    conn: SMBConnection, local_file_obj: io.BytesIO, nas_file_path: str
) -> bool:
    """Upload a file object to NAS."""
    try:
        # Create parent directory if needed
        parent_dir = "/".join(nas_file_path.split("/")[:-1])
        if parent_dir:
            nas_create_directory(conn, parent_dir)

        # Upload file
        local_file_obj.seek(0)  # Reset file pointer
        conn.storeFile(os.getenv("NAS_SHARE_NAME"), nas_file_path, local_file_obj)

        log_execution(
            f"Successfully uploaded file to NAS: {nas_file_path}",
            {"file_path": nas_file_path, "file_size": len(local_file_obj.getvalue())},
        )
        return True
    except Exception as e:
        log_error(
            f"Failed to upload file to NAS {nas_file_path}: {e}",
            "nas_upload",
            {"file_path": nas_file_path, "error_details": str(e)},
        )
        return False


def validate_config_structure(config: Dict[str, Any]) -> None:
    """Validate that configuration contains required sections and fields."""
    
    # Required top-level sections
    required_sections = [
        "api_settings",
        "monitored_institutions",
        "ssl_cert_path",
        "street_account_news",
    ]

    for section in required_sections:
        if section not in config:
            error_msg = f"Missing required configuration section: {section}"
            log_error(error_msg, "config_validation", {"missing_section": section})
            raise ValueError(error_msg)

    # Validate api_settings structure
    required_api_settings = [
        "categories",
        "pagination_limit",
        "pagination_offset",
        "request_delay",
        "max_retries",
        "retry_delay",
        "use_exponential_backoff",
        "max_backoff_delay",
        "lookback_days",
    ]
    for setting in required_api_settings:
        if setting not in config["api_settings"]:
            error_msg = f"Missing required API setting: {setting}"
            log_error(error_msg, "config_validation", {"missing_setting": setting})
            raise ValueError(error_msg)

    # Validate monitored_institutions is not empty
    if not config["monitored_institutions"]:
        error_msg = "monitored_institutions cannot be empty"
        log_error(error_msg, "config_validation", {})
        raise ValueError(error_msg)

    # Validate street_account_news section
    required_street_account_settings = ["output_data_path", "output_logs_path"]
    for setting in required_street_account_settings:
        if setting not in config["street_account_news"]:
            error_msg = f"Missing required street_account_news setting: {setting}"
            log_error(error_msg, "config_validation", {"missing_setting": setting})
            raise ValueError(error_msg)

    # Validate ssl_cert_path is not empty
    if not config["ssl_cert_path"] or not config["ssl_cert_path"].strip():
        error_msg = "ssl_cert_path cannot be empty"
        log_error(error_msg, "config_validation", {})
        raise ValueError(error_msg)

    log_execution(
        "Configuration structure validation passed",
        {
            "sections_validated": required_sections,
            "api_settings_validated": required_api_settings,
            "total_institutions": len(config["monitored_institutions"]),
        },
    )


def load_config_from_nas(nas_conn: SMBConnection) -> Dict[str, Any]:
    """Load and validate YAML configuration from NAS."""
    global logger

    try:
        config_path = os.getenv("CONFIG_PATH")
        logger.info(
            f"Loading YAML configuration from NAS: {sanitize_url_for_logging(config_path)}"
        )

        config_data = nas_download_file(nas_conn, config_path)
        if not config_data:
            error_msg = f"Failed to download configuration file from NAS: {sanitize_url_for_logging(config_path)}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        # Parse YAML configuration
        try:
            config = yaml.safe_load(config_data.decode("utf-8"))
        except yaml.YAMLError as e:
            error_msg = f"Invalid YAML in configuration file: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Validate configuration structure
        validate_config_structure(config)

        # Add NAS configuration from environment
        config["nas_share_name"] = os.getenv("NAS_SHARE_NAME")

        logger.info(
            f"Successfully loaded YAML configuration with {len(config['monitored_institutions'])} institutions"
        )
        return config

    except Exception as e:
        logger.error(f"Error loading configuration from NAS: {e}")
        raise


def load_config_locally() -> Dict[str, Any]:
    """Load and validate YAML configuration from local file (for testing)."""
    global logger

    try:
        config_path = "config.yaml"
        logger.info(f"Loading YAML configuration locally: {config_path}")

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Validate configuration structure
        validate_config_structure(config)

        # Add NAS configuration from environment
        config["nas_share_name"] = os.getenv("NAS_SHARE_NAME", "test_share")

        logger.info(
            f"Successfully loaded YAML configuration with {len(config['monitored_institutions'])} institutions"
        )
        return config

    except Exception as e:
        logger.error(f"Error loading configuration locally: {e}")
        raise


def setup_ssl_certificate(nas_conn: SMBConnection) -> Optional[str]:
    """Download SSL certificate from NAS and set up for API use."""
    global logger, config

    try:
        cert_path = config["ssl_cert_path"]
        logger.info(
            f"Downloading SSL certificate from NAS: {sanitize_url_for_logging(cert_path)}"
        )

        cert_data = nas_download_file(nas_conn, cert_path)
        if not cert_data:
            error_msg = f"Failed to download SSL certificate from NAS: {sanitize_url_for_logging(cert_path)}"
            logger.error(error_msg)
            return None

        # Create temporary certificate file
        temp_cert = tempfile.NamedTemporaryFile(mode="wb", suffix=".cer", delete=False)
        temp_cert.write(cert_data)
        temp_cert.close()

        # Set environment variables for SSL
        os.environ["REQUESTS_CA_BUNDLE"] = temp_cert.name
        os.environ["SSL_CERT_FILE"] = temp_cert.name

        logger.info(f"SSL certificate configured successfully: {temp_cert.name}")
        return temp_cert.name

    except Exception as e:
        logger.error(f"Error setting up SSL certificate: {e}")
        return None


def setup_proxy_configuration() -> str:
    """Configure proxy URL for API authentication."""
    global logger

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

        logger.info("Proxy configuration completed successfully")
        return proxy_url_formatted

    except Exception as e:
        logger.error(f"Error configuring proxy: {e}")
        raise


def setup_factset_api_client(proxy_url: str, ssl_cert_path: str):
    """Configure FactSet Street Account News API client with proxy and SSL settings."""
    global logger

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

        logger.info("FactSet Street Account News API client configured successfully")
        return configuration

    except Exception as e:
        logger.error(f"Error setting up FactSet API client: {e}")
        raise


def cleanup_temporary_files(ssl_cert_path: Optional[str]) -> None:
    """Clean up temporary files."""
    global logger

    if ssl_cert_path:
        try:
            os.unlink(ssl_cert_path)
            logger.info("Temporary SSL certificate file cleaned up")
        except (OSError, FileNotFoundError) as e:
            logger.warning(f"Failed to clean up SSL certificate file: {e}")


# ===== UTILITY FUNCTIONS =====


def nas_path_join(*parts: str) -> str:
    """Join path parts for NAS paths using forward slashes."""
    return "/".join(str(part) for part in parts if part)


def nas_file_exists(conn: SMBConnection, file_path: str) -> bool:
    """Check if a file or directory exists on the NAS."""
    global logger
    try:
        conn.getAttributes(os.getenv("NAS_SHARE_NAME"), file_path)
        return True
    except Exception:
        return False


def nas_create_directory(conn: SMBConnection, dir_path: str) -> bool:
    """Create directory on NAS with safe iterative parent creation."""
    global logger

    normalized_path = dir_path.strip("/").rstrip("/")
    if not normalized_path:
        logger.error("Cannot create directory with empty path")
        return False

    path_parts = [part for part in normalized_path.split("/") if part]
    if not path_parts:
        logger.error("Cannot create directory with invalid path")
        return False

    current_path = ""
    for part in path_parts:
        current_path = f"{current_path}/{part}" if current_path else part

        if nas_file_exists(conn, current_path):
            continue

        try:
            conn.createDirectory(os.getenv("NAS_SHARE_NAME"), current_path)
            logger.debug(f"Created directory: {sanitize_url_for_logging(current_path)}")
        except Exception as e:
            if not nas_file_exists(conn, current_path):
                logger.error(
                    f"Failed to create directory {sanitize_url_for_logging(current_path)}: {e}"
                )
                return False

    return True


def nas_create_directory_recursive(nas_conn: SMBConnection, dir_path: str) -> bool:
    """Create directory on NAS with recursive parent creation."""
    # Normalize and validate path
    normalized_path = dir_path.strip("/").rstrip("/")
    if not normalized_path:
        log_error("Cannot create directory with empty path", "directory_creation", {})
        return False

    # Split path into components
    path_parts = [part for part in normalized_path.split("/") if part]
    if not path_parts:
        log_error("Cannot create directory with invalid path", "directory_creation", {})
        return False

    # Build path incrementally from root
    current_path = ""
    for part in path_parts:
        current_path = f"{current_path}/{part}" if current_path else part

        # Check if directory already exists
        try:
            # Try to list directory contents to check if it exists
            nas_conn.listPath(config["nas_share_name"], current_path)
            continue  # Directory exists, move to next part
        except:
            # Directory doesn't exist, try to create it
            try:
                nas_conn.createDirectory(config["nas_share_name"], current_path)
                log_execution(
                    f"Created directory: {current_path}",
                    {"directory_path": current_path},
                )
            except Exception as e:
                # If creation fails, check if it exists now (race condition)
                try:
                    nas_conn.listPath(config["nas_share_name"], current_path)
                    continue  # Directory exists now
                except:
                    log_error(
                        f"Failed to create directory {current_path}: {e}",
                        "directory_creation",
                        {"directory_path": current_path, "error_details": str(e)},
                    )
                    return False

    return True


def sanitize_url_for_logging(url: str) -> str:
    """Remove auth tokens from URLs before logging."""
    if not url:
        return url

    # Remove authorization tokens and credentials from URL
    sanitized = re.sub(
        r"(password|token|auth)=[^&]*", r"\1=***", url, flags=re.IGNORECASE
    )
    sanitized = re.sub(r"://[^@]*@", "://***:***@", sanitized)
    return sanitized


def calculate_date_range() -> Tuple[datetime, datetime]:
    """Calculate date range for news retrieval based on lookback_days."""
    global config
    
    lookback_days = config["api_settings"].get("lookback_days", 30)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)
    
    log_console(
        f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} "
        f"({lookback_days} days)"
    )
    
    log_execution(
        "Calculated date range for news retrieval",
        {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "lookback_days": lookback_days,
        },
    )
    
    return start_date, end_date


# ===== CORE BUSINESS LOGIC FUNCTIONS =====


def get_available_filters(api_instance) -> Dict[str, Any]:
    """Retrieve all available filters from the API."""
    global config
    
    try:
        log_console("Retrieving available filters from API...")
        
        # Get all filters
        filters_response = api_instance.get_street_account_filters(
            structured=True,
            flattened=True
        )
        
        # Process and log filter information
        filter_summary = {
            "categories": [],
            "topics": [],
            "regions": [],
            "sectors": [],
            "watchlists": []
        }
        
        if filters_response and hasattr(filters_response, 'data'):
            data = filters_response.data
            
            # Extract flattened filters for easier use
            if hasattr(data, 'flattened_filters'):
                flattened = data.flattened_filters
                
                if hasattr(flattened, 'categories') and flattened.categories:
                    filter_summary["categories"] = [
                        cat.name for cat in flattened.categories 
                        if hasattr(cat, 'name')
                    ]
                
                if hasattr(flattened, 'topics') and flattened.topics:
                    filter_summary["topics"] = [
                        topic.name for topic in flattened.topics 
                        if hasattr(topic, 'name')
                    ]
                
                if hasattr(flattened, 'regions') and flattened.regions:
                    filter_summary["regions"] = [
                        region.name for region in flattened.regions 
                        if hasattr(region, 'name')
                    ]
                
                if hasattr(flattened, 'sectors') and flattened.sectors:
                    filter_summary["sectors"] = [
                        sector.name for sector in flattened.sectors 
                        if hasattr(sector, 'name')
                    ]
        
        log_execution(
            "Successfully retrieved available filters",
            {
                "categories_count": len(filter_summary["categories"]),
                "topics_count": len(filter_summary["topics"]),
                "regions_count": len(filter_summary["regions"]),
                "sectors_count": len(filter_summary["sectors"]),
                "sample_categories": filter_summary["categories"][:5],
                "sample_topics": filter_summary["topics"][:5],
            },
        )
        
        return filter_summary
        
    except Exception as e:
        log_error(
            f"Failed to retrieve filters: {e}",
            "filter_retrieval",
            {"error_details": str(e)},
        )
        return {}


def get_news_for_ticker(
    api_instance,
    ticker: str,
    institution_info: Dict[str, str],
    start_date: datetime,
    end_date: datetime,
) -> List[Dict[str, Any]]:
    """Get news headlines for a specific ticker."""
    global config
    
    for attempt in range(config["api_settings"]["max_retries"]):
        try:
            log_execution(
                f"Querying news for {ticker} (attempt {attempt + 1})",
                {
                    "ticker": ticker,
                    "institution": institution_info["name"],
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "attempt": attempt + 1,
                },
            )
            
            # Prepare request body - NO FILTERING to get ALL news
            headlines_request_data = HeadlinesRequestData(
                tickers=[
                    HeadlinesRequestTickersObject(
                        value=ticker,
                        type="Equity"
                    )
                ],
                # Remove is_primary filter to get ALL mentions
                search_time=HeadlinesRequestDataSearchTime(
                    start=start_date,
                    end=end_date
                )
            )
            
            # Only add filters if they are not empty arrays
            categories = config["api_settings"].get("categories", [])
            if categories:
                headlines_request_data.categories = categories
                
            topics = config["api_settings"].get("topics", [])
            if topics:
                headlines_request_data.topics = topics
                
            regions = config["api_settings"].get("regions", [])
            if regions:
                headlines_request_data.regions = regions
                
            sectors = config["api_settings"].get("sectors", [])
            if sectors:
                headlines_request_data.sectors = sectors
            
            headlines_request = HeadlinesRequest(
                data=headlines_request_data,
                meta=HeadlinesRequestMeta(
                    pagination=HeadlinesRequestMetaPagination(
                        limit=config["api_settings"]["pagination_limit"],
                        offset=config["api_settings"]["pagination_offset"]
                    ),
                    attributes=config["street_account_news"].get("response_attributes", [])
                )
            )
            
            # Make API call
            response = api_instance.get_street_account_headlines(
                headlines_request=headlines_request
            )
            
            if not response or not hasattr(response, "data") or not response.data:
                log_execution(f"No news found for {ticker}", {"ticker": ticker})
                return []
            
            # Convert response to list of dictionaries
            news_items = []
            for item in response.data:
                news_dict = item.to_dict() if hasattr(item, 'to_dict') else {}
                news_items.append(news_dict)
            
            log_execution(
                f"Successfully retrieved news for {ticker}",
                {
                    "ticker": ticker,
                    "news_count": len(news_items),
                    "date_range": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                },
            )
            
            return news_items
            
        except Exception as e:
            if attempt < config["api_settings"]["max_retries"] - 1:
                # Calculate delay with exponential backoff if enabled
                if config["api_settings"].get("use_exponential_backoff", False):
                    base_delay = config["api_settings"]["retry_delay"]
                    max_delay = config["api_settings"].get("max_backoff_delay", 60.0)
                    exponential_delay = base_delay * (2 ** attempt)
                    actual_delay = min(exponential_delay, max_delay)
                    log_console(
                        f"API query attempt {attempt + 1} failed for {ticker}, retrying in {actual_delay:.1f}s (exponential backoff): {e}",
                        "WARNING",
                    )
                else:
                    actual_delay = config["api_settings"]["retry_delay"]
                    log_console(
                        f"API query attempt {attempt + 1} failed for {ticker}, retrying in {actual_delay:.1f}s: {e}",
                        "WARNING",
                    )
                
                time.sleep(actual_delay)
            else:
                log_error(
                    f"Failed to query news for {ticker} after {attempt + 1} attempts: {e}",
                    "api_query",
                    {
                        "ticker": ticker,
                        "error_details": str(e),
                        "attempts": attempt + 1,
                    },
                )
                return []
    
    return []


def save_news_to_file(
    nas_conn: Optional[SMBConnection],
    ticker: str,
    institution_info: Dict[str, str],
    news_items: List[Dict[str, Any]],
    output_format: str = "both"
) -> bool:
    """Save news items to JSON and/or Excel file."""
    global config
    
    try:
        if not news_items:
            log_console(f"No news to save for {ticker}")
            return True
        
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        # Prepare data for saving
        df = pd.DataFrame(news_items)
        
        # Add metadata columns
        df['ticker'] = ticker
        df['institution_name'] = institution_info['name']
        df['retrieval_timestamp'] = timestamp
        
        # Reorder columns if they exist - using CORRECT field names
        priority_columns = ['ticker', 'institution_name', 'headlines', 'storyTime', 'id', 'primarySymbols', 'symbols']
        existing_priority = [col for col in priority_columns if col in df.columns]
        other_columns = [col for col in df.columns if col not in priority_columns]
        df = df[existing_priority + other_columns]
        
        if nas_conn:
            # Save to NAS
            data_path = config["street_account_news"]["output_data_path"]
            ticker_path = nas_path_join(data_path, ticker.replace("-", "_"))
            nas_create_directory_recursive(nas_conn, ticker_path)
            
            # Save JSON
            if output_format in ["json", "both"]:
                json_filename = f"{ticker}_news_{timestamp}.json"
                json_path = nas_path_join(ticker_path, json_filename)
                json_content = df.to_json(orient='records', indent=2, date_format='iso')
                json_obj = io.BytesIO(json_content.encode('utf-8'))
                
                if nas_upload_file(nas_conn, json_obj, json_path):
                    log_console(f"Saved JSON: {json_filename}")
            
            # Save Excel
            if output_format in ["excel", "both"]:
                excel_filename = f"{ticker}_news_{timestamp}.xlsx"
                excel_path = nas_path_join(ticker_path, excel_filename)
                excel_buffer = io.BytesIO()
                
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='News')
                
                excel_buffer.seek(0)
                if nas_upload_file(nas_conn, excel_buffer, excel_path):
                    log_console(f"Saved Excel: {excel_filename}")
        else:
            # Save locally for testing
            output_dir = Path("output") / ticker.replace("-", "_")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Save JSON
            if output_format in ["json", "both"]:
                json_file = output_dir / f"{ticker}_news_{timestamp}.json"
                df.to_json(json_file, orient='records', indent=2, date_format='iso')
                log_console(f"Saved locally: {json_file}")
            
            # Save Excel
            if output_format in ["excel", "both"]:
                excel_file = output_dir / f"{ticker}_news_{timestamp}.xlsx"
                with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='News')
                log_console(f"Saved locally: {excel_file}")
        
        log_execution(
            f"Successfully saved news for {ticker}",
            {
                "ticker": ticker,
                "news_count": len(news_items),
                "output_format": output_format,
                "timestamp": timestamp,
            },
        )
        
        return True
        
    except Exception as e:
        log_error(
            f"Failed to save news for {ticker}: {e}",
            "save_news",
            {
                "ticker": ticker,
                "error_details": str(e),
            },
        )
        return False


def create_summary_report(
    nas_conn: Optional[SMBConnection],
    all_news_summary: Dict[str, Any]
) -> None:
    """Create a summary report of all news retrieved."""
    global config
    
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        # Create summary DataFrame
        summary_data = []
        for ticker, info in all_news_summary.items():
            summary_data.append({
                'ticker': ticker,
                'institution_name': info['institution_name'],
                'news_count': info['news_count'],
                'date_range': info['date_range'],
                'primary_mentions': info.get('primary_mentions', 0),
                'all_mentions': info.get('all_mentions', 0),
                'earliest_news': info.get('earliest_news', ''),
                'latest_news': info.get('latest_news', ''),
            })
        
        df = pd.DataFrame(summary_data)
        
        # Add totals row
        totals = {
            'ticker': 'TOTAL',
            'institution_name': '',
            'news_count': df['news_count'].sum(),
            'date_range': f"{config['api_settings']['lookback_days']} days",
            'primary_mentions': df['primary_mentions'].sum() if 'primary_mentions' in df.columns else 0,
            'all_mentions': df['all_mentions'].sum() if 'all_mentions' in df.columns else 0,
            'earliest_news': '',
            'latest_news': '',
        }
        df = pd.concat([df, pd.DataFrame([totals])], ignore_index=True)
        
        if nas_conn:
            # Save to NAS
            summary_path = config["street_account_news"]["output_data_path"]
            summary_filename = f"canadian_banks_news_summary_{timestamp}.xlsx"
            summary_file_path = nas_path_join(summary_path, summary_filename)
            
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Summary')
            
            excel_buffer.seek(0)
            if nas_upload_file(nas_conn, excel_buffer, summary_file_path):
                log_console(f"Summary report saved: {summary_filename}")
        else:
            # Save locally
            output_dir = Path("output")
            output_dir.mkdir(exist_ok=True)
            summary_file = output_dir / f"canadian_banks_news_summary_{timestamp}.xlsx"
            
            with pd.ExcelWriter(summary_file, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Summary')
            
            log_console(f"Summary report saved locally: {summary_file}")
        
        log_execution(
            "Summary report created",
            {
                "total_institutions": len(all_news_summary),
                "total_news_items": df['news_count'].sum() - totals['news_count'],  # Exclude total row
                "timestamp": timestamp,
            },
        )
        
    except Exception as e:
        log_error(
            f"Failed to create summary report: {e}",
            "summary_report",
            {"error_details": str(e)},
        )


# ===== MAIN FUNCTION =====


def main(use_nas: bool = True) -> None:
    """Main function to retrieve Street Account news for Canadian banks."""
    global logger, config

    # Initialize logging
    logger = setup_logging()
    start_time = datetime.now()

    log_console("=== STREET ACCOUNT NEWS - CANADIAN BANKS ===")
    log_execution(
        "Street Account News retrieval started",
        {
            "start_time": start_time.isoformat(),
            "stage": "street_account_news_canadian_banks",
            "use_nas": use_nas,
        },
    )

    nas_conn = None
    ssl_cert_path = None
    stage_summary = {
        "status": "unknown",
        "total_institutions": 0,
        "total_news_items": 0,
        "execution_time_seconds": 0,
        "errors": {},
    }

    try:
        if use_nas:
            # Step 1: Validate environment variables
            log_console("Validating environment variables...")
            validate_environment_variables()

            # Step 2: Connect to NAS
            log_console("Connecting to NAS...")
            nas_conn = get_nas_connection()
            if not nas_conn:
                raise RuntimeError("Failed to establish NAS connection")

            # Step 3: Load configuration from NAS
            log_console("Loading configuration from NAS...")
            config = load_config_from_nas(nas_conn)
        else:
            # Load configuration locally for testing
            log_console("Loading configuration locally for testing...")
            config = load_config_locally()
            
        stage_summary["total_institutions"] = len(config["monitored_institutions"])

        if use_nas:
            # Step 4: Set up SSL certificate
            log_console("Setting up SSL certificate...")
            ssl_cert_path = setup_ssl_certificate(nas_conn)
            if not ssl_cert_path:
                raise RuntimeError("Failed to set up SSL certificate")

            # Step 5: Configure proxy
            log_console("Configuring proxy authentication...")
            proxy_url = setup_proxy_configuration()

            # Step 6: Set up FactSet API client
            log_console("Setting up FactSet Street Account News API client...")
            api_configuration = setup_factset_api_client(proxy_url, ssl_cert_path)
        else:
            # For local testing - simplified setup
            log_console("Setting up API client for local testing...")
            api_configuration = fds.sdk.StreetAccountNews.Configuration(
                username=os.getenv("API_USERNAME", "test_user"),
                password=os.getenv("API_PASSWORD", "test_pass"),
            )

        log_console("Setup complete - ready for API calls")

        # Step 7: Calculate date range
        log_console("Calculating date range for news retrieval...")
        start_date, end_date = calculate_date_range()

        # Step 8: Process each institution
        log_console("Processing Canadian banks for news retrieval...")
        all_news_summary = {}
        total_news_count = 0

        with fds.sdk.StreetAccountNews.ApiClient(api_configuration) as api_client:
            # Create API instances
            headlines_api_instance = headlines_api.HeadlinesApi(api_client)
            filters_api_instance = filters_api.FiltersApi(api_client)
            
            # Get available filters (optional - for information)
            available_filters = get_available_filters(filters_api_instance)
            
            # Process each Canadian bank
            for i, (ticker, institution_info) in enumerate(
                config["monitored_institutions"].items(), 1
            ):
                log_console(
                    f"Processing {ticker} ({i}/{len(config['monitored_institutions'])})..."
                )
                
                # Get news for this ticker
                news_items = get_news_for_ticker(
                    headlines_api_instance,
                    ticker,
                    institution_info,
                    start_date,
                    end_date,
                )
                
                # Save news data
                if news_items:
                    save_news_to_file(
                        nas_conn,
                        ticker,
                        institution_info,
                        news_items,
                        output_format="both"
                    )
                    
                    # Extract summary information - using CORRECT field name
                    news_dates = [item.get('storyTime', '') for item in news_items]
                    news_dates = [d for d in news_dates if d]  # Filter out empty dates
                    
                    all_news_summary[ticker] = {
                        'institution_name': institution_info['name'],
                        'news_count': len(news_items),
                        'date_range': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                        'earliest_news': min(news_dates) if news_dates else '',
                        'latest_news': max(news_dates) if news_dates else '',
                        'primary_mentions': sum(1 for item in news_items if ticker in item.get('primarySymbols', [])),
                        'all_mentions': len(news_items),
                    }
                    
                    total_news_count += len(news_items)
                    
                    log_console(
                        f"{ticker}: Retrieved {len(news_items)} news items"
                    )
                else:
                    all_news_summary[ticker] = {
                        'institution_name': institution_info['name'],
                        'news_count': 0,
                        'date_range': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                        'earliest_news': '',
                        'latest_news': '',
                        'primary_mentions': 0,
                        'all_mentions': 0,
                    }
                    
                    log_console(f"{ticker}: No news found")
                
                # Rate limiting between requests
                if i < len(config["monitored_institutions"]):
                    time.sleep(config["api_settings"]["request_delay"])

        # Step 9: Create summary report
        log_console("Creating summary report...")
        create_summary_report(nas_conn, all_news_summary)

        # Update stage summary
        stage_summary["total_news_items"] = total_news_count
        stage_summary["status"] = "completed"
        stage_summary["execution_time_seconds"] = (
            datetime.now() - start_time
        ).total_seconds()
        
        log_console(
            f"News retrieval complete: {total_news_count} total news items across "
            f"{len(config['monitored_institutions'])} institutions"
        )

    except Exception as e:
        stage_summary["status"] = "failed"
        stage_summary["execution_time_seconds"] = (
            datetime.now() - start_time
        ).total_seconds()
        stage_summary["errors"]["main_execution"] = str(e)

        log_console(f"News retrieval failed: {e}", "ERROR")
        log_error(
            f"Main execution failed: {e}",
            "main_execution",
            {
                "error_details": str(e),
                "execution_time_seconds": stage_summary["execution_time_seconds"],
            },
        )
        raise

    finally:
        # Save logs to NAS
        if nas_conn:
            try:
                save_logs_to_nas(nas_conn, stage_summary)
            except Exception as e:
                log_console(f"Warning: Failed to save logs to NAS: {e}", "WARNING")

        # Cleanup
        if nas_conn:
            nas_conn.close()
            log_console("NAS connection closed")

        cleanup_temporary_files(ssl_cert_path)
        log_console("=== STREET ACCOUNT NEWS RETRIEVAL COMPLETE ===")


if __name__ == "__main__":
    # Check if we should use NAS or run locally
    import sys
    use_nas = "--local" not in sys.argv
    
    if not use_nas:
        print("Running in LOCAL mode - will not connect to NAS")
    
    main(use_nas=use_nas)
# FactSet Street Account News API Documentation

## Overview
This document describes the Street Account News API response structure and available fields when retrieving news for Canadian banks.

## API Endpoints Used

### 1. Headlines API (`/headlines`)
Main endpoint for retrieving news headlines and stories.

### 2. Filters API (`/filters`)
Endpoint for retrieving available filter options (categories, topics, regions, sectors).

### 3. Views API (`/views`)
Endpoint for creating and managing saved news views (optional).

## Request Parameters

### Ticker Configuration
```python
tickers = [
    {
        "value": "RY-CA",  # Ticker symbol
        "type": "Equity"   # Must be one of: Index, ETF, Mutual_Fund, Portfolios, Equity, privateCompanies, Fixed_Income, Holder
    }
]
```

### Available Filters

#### Categories (News Types)
- Earnings
- Guidance
- Mergers & Acquisitions
- Regulatory
- Credit Ratings
- Management Changes
- Products & Services
- Markets

#### Topics
- Earnings
- Banking
- Capital Markets
- Economic Commentary

#### Regions
- North America
- Canada

#### Sectors
- Financial
- Banking

## Response Structure

### Headlines Response Fields (CORRECTED)

| Field | Type | Description |
|-------|------|-------------|
| `headlines` | string | The news headline text |
| `storyTime` | datetime | When the story was published |
| `id` | string | Unique identifier for the story |
| `primarySymbols` | array | Primary ticker symbols |
| `symbols` | array | All ticker symbols mentioned |
| `subjects` | array | Companies/entities mentioned |
| `storyBody` | string | Full story body content |
| `referenceUris` | string | Reference URIs |
| `url` | string | URL to the story |

### Sample Response Structure (CORRECTED)
```json
{
    "data": [
        {
            "headlines": "Royal Bank Q3 Earnings Beat Estimates",
            "storyTime": "2025-08-13T14:30:00Z",
            "id": "SA123456789",
            "primarySymbols": ["RY-CA"],
            "symbols": ["RY-CA", "TD-CA", "BMO-CA"],
            "subjects": ["Royal Bank of Canada", "Banking Sector"],
            "storyBody": "Full text of the news story...",
            "referenceUris": "https://...",
            "url": "https://..."
        }
    ],
    "meta": {
        "pagination": {
            "total": 45,
            "limit": 100,
            "offset": 0
        }
    }
}
```

## Canadian Bank Tickers

| Ticker | Institution | Type |
|--------|------------|------|
| RY-CA | Royal Bank of Canada | Canadian_Banks |
| BMO-CA | Bank of Montreal | Canadian_Banks |
| CM-CA | Canadian Imperial Bank of Commerce | Canadian_Banks |
| NA-CA | National Bank of Canada | Canadian_Banks |
| BNS-CA | Bank of Nova Scotia | Canadian_Banks |
| TD-CA | Toronto-Dominion Bank | Canadian_Banks |
| LB-CA | Laurentian Bank | Canadian_Banks |

## Data Output Format

### JSON Output
- File naming: `{ticker}_news_{timestamp}.json`
- Location: `output/{ticker}/` (local) or NAS path
- Format: Array of news objects with all fields

### Excel Output
- File naming: `{ticker}_news_{timestamp}.xlsx`
- Location: `output/{ticker}/` (local) or NAS path
- Columns: All response fields plus metadata (ticker, institution_name, retrieval_timestamp)

### Summary Report
- File naming: `canadian_banks_news_summary_{timestamp}.xlsx`
- Contains:
  - Ticker
  - Institution name
  - News count
  - Date range
  - Categories found
  - Earliest/latest news dates

## Rate Limiting & Best Practices

1. **Request Delay**: 2 seconds between requests (configurable)
2. **Pagination**: Max 100 items per request
3. **Date Range**: Default 30 days lookback
4. **Retry Logic**: Exponential backoff with max 5 retries
5. **Primary Filter**: Use `isPrimary=true` to get only news where ticker is main subject

## Error Handling

The implementation includes:
- Environment variable validation
- SSL certificate verification
- Proxy authentication handling
- API retry with exponential backoff
- Detailed logging to NAS
- Local fallback mode for testing

## Usage Examples

### Local Testing (without NAS)
```bash
python street_account_news_canadian_banks.py --local
```

### Production Run (with NAS)
```bash
python street_account_news_canadian_banks.py
```

## Configuration Files

### .env
Contains credentials and connection settings:
- API credentials (API_USERNAME, API_PASSWORD)
- Proxy settings (PROXY_USER, PROXY_PASSWORD, PROXY_URL)
- NAS settings (NAS_USERNAME, NAS_PASSWORD, etc.)

### config.yaml
Contains:
- API settings (categories, topics, regions, sectors)
- Rate limiting configuration
- Output paths
- Canadian bank ticker list

## Logging

### Execution Log
- Location: `{output_logs_path}/street_account_news_canadian_banks_{timestamp}.json`
- Contains: All execution steps, API calls, and results

### Error Log
- Location: `{output_logs_path}/Errors/street_account_news_canadian_banks_errors_{timestamp}.json`
- Contains: Any errors encountered during execution

## Notes

1. The API uses "Equity" as the ticker type for stocks (not "Ticker")
2. Date/time fields are in ISO format
3. Some banks may have limited news depending on coverage
4. The `isPrimary` filter is important to avoid news where the bank is only mentioned
5. Categories and topics can be empty arrays if not classified
# FactSet Street Account News - Canadian Banks

Real-time news monitoring for Canadian financial institutions using FactSet Street Account News API.

## Overview

This implementation mirrors the structure and patterns from your Events and Transcripts example, but uses the Street Account News API to retrieve real-time news for Canadian banks.

## Setup Instructions

### 1. Environment Setup

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

#### Create .env file
```bash
cp .env.template .env
# Edit .env with your credentials:
# - FactSet API credentials
# - Proxy settings
# - NAS configuration
```

#### Review config.yaml
- Canadian bank tickers are pre-configured
- Adjust categories, topics, regions as needed
- Modify lookback_days for date range

### 3. Test Setup

```bash
# Test all imports and modules
python test_imports.py
```

## Usage

### Local Testing (without NAS connection)
```bash
python street_account_news_canadian_banks.py --local
```
This will:
- Load config.yaml locally
- Skip NAS connection
- Save output to local `output/` directory
- Not require SSL certificate or proxy

### Production Run (with NAS)
```bash
python street_account_news_canadian_banks.py
```
This will:
- Connect to NAS for config and SSL certificate
- Use proxy authentication
- Save output to NAS paths
- Generate execution and error logs

## Features

### Matches Example Implementation
✅ **Environment Variables** - Same structure as Events/Transcripts  
✅ **NAS Connection** - SMB connection with NTLM authentication  
✅ **SSL Certificate** - Downloads from NAS, sets up temp file  
✅ **Proxy Configuration** - NTLM proxy with domain authentication  
✅ **Config Loading** - YAML from NAS or local  
✅ **Logging** - Console, execution, and error logs  
✅ **Error Handling** - Retry with exponential backoff  
✅ **Directory Creation** - Recursive NAS directory creation  

### Street Account News Specific
📰 **Real-time News** - Latest news for Canadian banks  
🏦 **7 Canadian Banks** - RY, BMO, CM, NA, BNS, TD, LB  
📊 **Multiple Formats** - JSON and Excel output  
📈 **Summary Report** - Consolidated view of all banks  
🔍 **Smart Filtering** - Primary subject only  
📅 **Date Range** - Configurable lookback period  

## Output Structure

```
output/
├── RY_CA/
│   ├── RY-CA_news_2025-08-13_14-30-00.json
│   └── RY-CA_news_2025-08-13_14-30-00.xlsx
├── BMO_CA/
│   └── ...
└── canadian_banks_news_summary_2025-08-13_14-30-00.xlsx
```

## Key Differences from Events & Transcripts

| Aspect | Events & Transcripts | Street Account News |
|--------|---------------------|-------------------|
| Content Type | Quarterly transcripts | Real-time news |
| Frequency | Quarterly | Continuous |
| API Module | `fds.sdk.EventsandTranscripts` | `fds.sdk.StreetAccountNews` |
| Primary Endpoint | `get_transcripts_ids` | `get_street_account_headlines` |
| Date Logic | 3-year rolling window | 30-day lookback |
| Output | XML transcripts | JSON/Excel news items |

## Files

- `street_account_news_canadian_banks.py` - Main script
- `config.yaml` - Configuration file
- `.env.template` - Environment variables template
- `requirements.txt` - Python dependencies
- `test_imports.py` - Import testing utility
- `API_DOCUMENTATION.md` - API response structure
- `README_SETUP.md` - This file

## Troubleshooting

### Import Errors
```bash
# Ensure virtual environment is activated
source venv/bin/activate
```

### SSL Certificate Issues
- Verify certificate path in config.yaml
- Check NAS connection and permissions

### Proxy Authentication
- Verify PROXY_DOMAIN (usually "MAPLE")
- Check proxy credentials in .env

### No News Found
- Check ticker format (use "-CA" suffix)
- Verify date range in config
- Test with broader categories/topics

## Next Steps

1. **Test Locally**: Run with `--local` flag to verify setup
2. **Add Credentials**: Fill in .env with actual values
3. **Test Connection**: Remove `--local` to test NAS/proxy
4. **Review Output**: Check generated JSON/Excel files
5. **Schedule**: Set up cron/scheduler for regular runs

## Support

For issues or questions:
- Review `API_DOCUMENTATION.md` for field details
- Check execution logs for detailed traces
- Compare with original Events & Transcripts implementation
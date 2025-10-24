# Mocking Goose - Local Development

A local development version of Mocking Goose that runs on your machine instead of requiring Databricks Apps deployment.

## Quick Start

### 1. Setup (One Time)

```bash
./setup_local.sh
```

This will install:
- `uv` - Python package manager
- `goose` - AI assistant binary
- All Python dependencies
- Set up virtual environments for mock-and-roll and awesome-databricks-mcp

### 2. Run

```bash
./run_local.sh
```

### 3. Access

Open your browser to: **http://localhost:8000**

Enter your Databricks credentials:
- **Host**: `https://your-workspace.cloud.databricks.com/`
- **Token**: Your Personal Access Token (PAT)

Click **Launch Goose** and start creating demos!

## What You Need

- **Python 3.12 or 3.13**
- **macOS or Linux**
- **Databricks PAT** (Personal Access Token)

## Project Structure

```
mocking-goose-local/
├── proxy_app.py              # Original (for Databricks Apps)
├── proxy_app_local.py         # Local development version ← You'll use this
├── app.yaml                   # Databricks Apps config
├── setup_local.sh             # One-time setup script
├── run_local.sh               # Start the local server
├── .env.local.example         # Template for environment variables
├── .env.local                 # Your local config (gitignored)
├── requirements.txt           # Python dependencies
├── mock-and-roll/             # Synthetic data framework
├── awesome-databricks-mcp/    # MCP server with Databricks tools
└── ui-static/                 # Web UI assets
```

## Configuration

The `.env.local` file contains your local configuration. It's automatically created by `setup_local.sh`.

You can customize:
- `APP_PORT` - Change the port (default: 8000)
- `INACTIVITY_TIMEOUT_SECONDS` - Session timeout (default: 3600)
- Paths to mock-and-roll, goose binary, etc.

## Troubleshooting

**Port 8000 already in use?**
```bash
export APP_PORT=8001
./run_local.sh
```

**Can't find uv or goose?**
```bash
export PATH="$HOME/.local/bin:$PATH"
```

**Need to change paths?**

Edit `.env.local` file

## Features

- **AI-Generated Synthetic Data**: Create realistic, industry-specific datasets
- **Automated Pipeline Creation**: Build bronze → silver → gold data pipelines
- **Interactive Demo Interface**: Web-based UI for creating demos
- **MCP Integration**: 63 Databricks tools accessible via Model Context Protocol
- **Session Management**: Per-user backend instances with automatic cleanup

## How It Works

1. When you enter credentials in the UI, the proxy creates a new backend session
2. Each session gets:
   - Isolated working directory (fresh copy of mock-and-roll)
   - Dedicated Goose instance (running on a random port)
   - Private credentials (stored only in HTTP-only cookies)
   - Automatic cleanup after 1 hour of inactivity

3. Sessions are automatically cleaned up on logout or timeout

## Documentation

- **Quick Start**: See [QUICKSTART_LOCAL.md](QUICKSTART_LOCAL.md)
- **Full Guide**: See [LOCAL_DEVELOPMENT.md](LOCAL_DEVELOPMENT.md)
- **Original README**: See [README.md](README.md)

## Original Project

This is a local development version of the original Mocking Goose project.

**Created by**: Jeremy Herbert, Maaz Rahman & Xavier Armitage

**Powered by**:
- [mock-and-roll](https://github.com/zaxier/mock-and-roll) - AI-native demo framework
- [awesome-databricks-mcp](https://github.com/PulkitXChadha/awesome-databricks-mcp) - MCP server
- [Goose](https://block.github.io/goose/) - AI coding assistant

## License

See individual component licenses.

# Local Development Setup Guide

This guide will help you run Mocking Goose locally on your development machine instead of deploying it to Databricks Apps.

## Prerequisites

- **Operating System**: macOS or Linux
- **Python**: 3.12 or 3.13
- **Databricks Access**: Valid workspace URL and Personal Access Token (PAT)
- **Disk Space**: ~500MB for dependencies and temporary files

## Quick Start

### 1. Clone and Setup

```bash
# Navigate to the project directory
cd /path/to/mocking-goose

# Run the setup script (one-time setup)
./setup_local.sh
```

The setup script will:
- Install `uv` (Python package manager)
- Install `goose` (AI assistant binary)
- Install Python dependencies
- Setup `mock-and-roll` and `awesome-databricks-mcp` projects
- Create `.env.local` configuration file

### 2. Configure (Optional)

Edit `.env.local` if you need to customize paths:

```bash
# Example .env.local
LOCAL_MOCK_AND_ROLL_DIR=/Users/you/Projects/mocking-goose/mock-and-roll
GOOSE_BIN_DIR=/Users/you/.local/bin
APP_HOST=0.0.0.0
APP_PORT=8000
```

### 3. Run the App

```bash
./run_local.sh
```

The app will start on `http://localhost:8000`

### 4. Access the UI

1. Open your browser to `http://localhost:8000`
2. Enter your Databricks credentials:
   - **Databricks Host**: `https://your-workspace.cloud.databricks.com/`
   - **Databricks Token**: Your Personal Access Token (PAT)
3. Click "Launch Goose"
4. Start creating demos!

## Architecture

### Local vs Databricks Deployment

| Component | Databricks Apps | Local Development |
|-----------|----------------|-------------------|
| **Entry Point** | `app.yaml` → `proxy_app.py` | `run_local.sh` → `proxy_app_local.py` |
| **Paths** | Hardcoded `/app/python/source_code/...` | Configurable via `.env.local` |
| **Dependencies** | Auto-installed on startup | Pre-installed via `setup_local.sh` |
| **Config** | `/home/app/.config/goose/` | `~/.config/goose-local/` |
| **Credentials** | OAuth or PAT | PAT (entered in UI) |

### Key Differences in `proxy_app_local.py`

The local version has these modifications:

1. **Path Resolution**: Uses `Path(__file__).parent` for relative paths
2. **Environment Variables**: Loads from `.env.local` using `python-dotenv`
3. **Dependency Installation**: Skips auto-install (assumes setup was run)
4. **Configuration Directory**: Uses `~/.config/goose-local` to avoid conflicts
5. **Default Paths**: Points to local directories instead of `/app/python/...`

## Project Structure

```
mocking-goose/
├── proxy_app.py              # Original (for Databricks Apps)
├── proxy_app_local.py         # Local development version
├── app.yaml                   # Databricks Apps config
├── setup_local.sh             # One-time setup script
├── run_local.sh               # Start the local server
├── .env.local.example         # Template for environment variables
├── .env.local                 # Your local config (gitignored)
├── LOCAL_DEVELOPMENT.md       # This file
├── requirements.txt           # Python dependencies
├── mock-and-roll/             # Synthetic data framework
├── awesome-databricks-mcp/    # MCP server with Databricks tools
└── ui-static/                 # Web UI assets
```

## How It Works

### Session Management

1. When you enter credentials in the UI, the proxy creates a new backend session
2. Each session:
   - Copies `mock-and-roll` to a temporary directory
   - Runs `uv sync` to install dependencies
   - Starts a Goose instance with your Databricks credentials
   - Proxies all requests to your Goose instance

3. Sessions are automatically cleaned up after:
   - 1 hour of inactivity
   - Manual logout
   - Tab/browser closure

### Per-Session Isolation

Each user session gets:
- **Isolated working directory**: Fresh copy of mock-and-roll
- **Dedicated Goose instance**: Running on a random port
- **Private credentials**: Stored only in HTTP-only cookies
- **Automatic cleanup**: Temp files removed on session end

### Resource Usage

- **Memory**: ~200-500MB per active session (Goose instance)
- **CPU**: Variable, based on AI operations
- **Disk**: ~50MB per session (temporary copies)
- **Network**: HTTPS to your Databricks workspace

## Troubleshooting

### Setup Issues

**Problem**: `uv` or `goose` not found after setup

```bash
# Add to PATH manually
export PATH="$HOME/.local/bin:$PATH"

# Or restart your terminal
```

**Problem**: Python version not found

```bash
# macOS
brew install python@3.13

# Ubuntu/Debian
sudo apt install python3.13
```

### Runtime Issues

**Problem**: "Failed to start backend"

Check the console output for specific errors. Common causes:
- Invalid Databricks credentials
- `mock-and-roll` directory not found
- Missing dependencies

**Problem**: Session times out immediately

- Check your `.env.local` `INACTIVITY_TIMEOUT_SECONDS` setting
- Default is 3600 seconds (1 hour)

**Problem**: UI shows login screen after clicking "Launch Goose"

- Check browser console for errors
- Verify Databricks credentials are correct
- Check that port 8000 is not already in use

### Port Conflicts

If port 8000 is already in use:

```bash
# Edit .env.local
APP_PORT=8001

# Or set temporarily
export APP_PORT=8001
./run_local.sh
```

### Logs and Debugging

The app outputs logs to stdout. To capture them:

```bash
./run_local.sh 2>&1 | tee mocking-goose.log
```

To see what ports are in use:

```bash
# macOS/Linux
lsof -i :8000
netstat -an | grep 8000
```

## Development Workflow

### Making Changes

1. **UI Changes**: Edit files in `ui-static/`, refresh browser
2. **Backend Changes**: Edit `proxy_app_local.py`, restart server
3. **Mock-and-Roll Changes**: Edit files in `mock-and-roll/`, new sessions will pick up changes
4. **MCP Changes**: Edit files in `awesome-databricks-mcp/`, restart server

### Testing Changes

```bash
# Stop the server (Ctrl+C)
# Make your changes
# Restart
./run_local.sh
```

### Running Multiple Instances

You can run multiple instances on different ports:

```bash
# Terminal 1
export APP_PORT=8001
./run_local.sh

# Terminal 2
export APP_PORT=8002
./run_local.sh
```

## Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `LOCAL_MOCK_AND_ROLL_DIR` | `./mock-and-roll` | Path to mock-and-roll project |
| `GOOSE_BIN_DIR` | `~/.local/bin` | Directory containing goose binary |
| `GOOSE_BIN` | (none) | Direct path to goose binary (overrides DIR) |
| `AWESOME_DATABRICKS_MCP_DIR` | `./awesome-databricks-mcp` | Path to MCP server |
| `GOOSE_CONFIG_DIR` | `~/.config/goose-local` | Goose config directory |
| `APP_HOST` | `0.0.0.0` | Host to bind the server |
| `APP_PORT` | `8000` | Port to bind the server |
| `COOKIE_MAX_AGE_SECONDS` | `28800` (8 hours) | Cookie expiration time |
| `INACTIVITY_TIMEOUT_SECONDS` | `3600` (1 hour) | Session idle timeout |
| `COOKIE_SECURE` | `false` | Use secure cookies (set true for HTTPS) |

## Security Considerations

### Local Development

- Credentials are stored in HTTP-only cookies (not accessible to JavaScript)
- Each session is isolated with its own Goose instance
- Temporary files are cleaned up on session end
- The proxy only accepts connections from `localhost` by default

### Production Considerations

If you want to expose this to a network:

1. **Use HTTPS**: Set `COOKIE_SECURE=true` and use a reverse proxy
2. **Restrict Access**: Use firewall rules or authentication
3. **Set APP_HOST**: Change from `0.0.0.0` to specific interface
4. **Monitor Resources**: Multiple sessions can consume significant memory

## Health Check

The app provides a health endpoint:

```bash
curl http://localhost:8000/_health
```

Returns:
```json
{
  "status": "ok",
  "mode": "local_development",
  "uptime_seconds": 1234,
  "server": { "cpu_percent": 5.2, "mem_percent": 45.8, ... },
  "proxy_process": { "cpu_percent": 2.1, "rss_bytes": 52428800, ... },
  "goose": { "instances": 2, "cpu_percent_sum": 15.5, ... },
  "ws_connections": 2,
  "live_sessions": 2
}
```

## Comparison: Local vs Databricks Apps

### When to Use Local Development

✅ Testing changes quickly
✅ Development and debugging
✅ Learning the codebase
✅ Custom modifications
✅ Running without Databricks Apps access

### When to Use Databricks Apps

✅ Production deployment
✅ Sharing with team members
✅ Integrated with Databricks workspace
✅ Automatic scaling and management
✅ No local dependencies required

## Next Steps

- Read the main [README.md](README.md) for feature documentation
- Explore `mock-and-roll/` for synthetic data examples
- Check `awesome-databricks-mcp/` for available Databricks tools
- Customize the UI in `ui-static/`

## Getting Help

If you encounter issues:

1. Check the troubleshooting section above
2. Review console output for error messages
3. Verify all prerequisites are installed
4. Check that paths in `.env.local` are correct
5. Try running setup again: `./setup_local.sh`

## Cleaning Up

To remove all local setup:

```bash
# Stop the server (Ctrl+C)

# Remove dependencies (optional)
rm -rf ~/.local/bin/uv
rm -rf ~/.local/bin/goose
rm -rf ~/.config/goose-local

# Remove virtual environments
cd mock-and-roll && rm -rf .venv
cd ../awesome-databricks-mcp && rm -rf .venv

# Remove local config
rm .env.local
```

## Contributing

When making changes for local development:

1. **Don't modify** `proxy_app.py` or `app.yaml` (used for Databricks Apps)
2. **Do modify** `proxy_app_local.py` for local-specific changes
3. **Update** this documentation if adding new features
4. **Test** both local and Databricks Apps deployment if possible

---

**Happy Hacking!** 🦆

# Quick Start - Local Development

Run Mocking Goose on your local machine in 3 simple steps:

## 1. Setup (One Time)

```bash
./setup_local.sh
```

This installs all dependencies and creates your local configuration.

## 2. Run

```bash
./run_local.sh
```

## 3. Access

Open your browser to: **http://localhost:8000**

Enter your Databricks credentials:
- **Host**: `https://your-workspace.cloud.databricks.com/`
- **Token**: Your Personal Access Token (PAT)

Click **Launch Goose** and start creating demos!

---

## What You Need

- **Python 3.12 or 3.13**
- **macOS or Linux**
- **Databricks PAT** (Personal Access Token)

---

## Troubleshooting

**Can't find uv or goose?**
```bash
export PATH="$HOME/.local/bin:$PATH"
```

**Port 8000 already in use?**
```bash
export APP_PORT=8001
./run_local.sh
```

**Need to change paths?**

Edit `.env.local` file

---

## Files Created

- `.env.local` - Your local configuration
- `proxy_app_local.py` - Local version of the app
- `setup_local.sh` - Setup script
- `run_local.sh` - Run script
- `LOCAL_DEVELOPMENT.md` - Full documentation

## Original Files (Unchanged)

- `proxy_app.py` - For Databricks Apps deployment
- `app.yaml` - Databricks Apps configuration
- `README.md` - Main project documentation

---

For detailed documentation, see [LOCAL_DEVELOPMENT.md](LOCAL_DEVELOPMENT.md)

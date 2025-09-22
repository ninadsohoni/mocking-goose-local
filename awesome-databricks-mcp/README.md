# Databricks MCP Server (stdio-only)

A simplified stdio-only MCP server for Databricks integration. Provides 63 tools and 2 prompts for direct use with MCP clients like goose.

## 🚀 Quick Start

```bash
# 1. Setup
uv sync

# 2. Run with your credentials
DATABRICKS_HOST=<workspace-url> DATABRICKS_TOKEN=<token> uv run run_mcp_stdio.py
```

## 🛠️ Features

- **63 Databricks Tools**: SQL, Unity Catalog, Jobs, Pipelines, Dashboards
- **2 Prompt Templates**: LDP Pipeline & Lakeview Dashboard builders  
- **Minimal Dependencies**: Only fastmcp, databricks-sdk, pyyaml
- **Direct Integration**: Works with any MCP client via stdio

## 📋 Requirements

- Python 3.11+
- uv package manager
- Databricks workspace + Personal Access Token

## 🎯 Usage

Access tools like `list_warehouses`, `execute_dbsql`, or prompts like `get_prompt_build_ldp_pipeline`.

Perfect for integration with goose, Claude Desktop, or any MCP-compatible client.

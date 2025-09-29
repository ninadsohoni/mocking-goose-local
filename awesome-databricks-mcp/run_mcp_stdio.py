#!/usr/bin/env python3
"""Direct stdio MCP server for Databricks without HTTP layer.

This runs the MCP server directly with stdio transport, bypassing the HTTP/FastAPI layer.
Perfect for local development when you only need MCP functionality.

Usage:
    python run_mcp_stdio.py

Environment variables required:
    DATABRICKS_HOST - Your Databricks workspace URL
    DATABRICKS_TOKEN - Your Personal Access Token
"""

import os
import sys
from pathlib import Path

# Add the current directory to Python path so we can import server modules
sys.path.insert(0, str(Path(__file__).parent))

import yaml
from fastmcp import FastMCP

from server.prompts import load_prompts
from server.tools import load_tools


def load_config() -> dict:
    """Load configuration from config.yaml."""
    config_path = Path(__file__).parent / 'config.yaml'
    if config_path.exists():
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    return {}


def main():
    """Run the MCP server with stdio transport."""
    # Ensure we're in the correct directory for relative paths to work
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    # Allow DATABRICKS_HOST and DATABRICKS_TOKEN to be passed as command-line arguments
    import argparse

    parser = argparse.ArgumentParser(description="Run MCP server with Databricks credentials.")
    parser.add_argument('--databricks-host', type=str, help='Databricks workspace URL')
    parser.add_argument('--databricks-token', type=str, help='Databricks Personal Access Token')
    args, unknown = parser.parse_known_args()

    # Set environment variables from command-line arguments if provided
    
    if args.databricks_host:
        os.environ['DATABRICKS_HOST'] = args.databricks_host
    if args.databricks_token:
        os.environ['DATABRICKS_TOKEN'] = args.databricks_token

    # Check for required environment variables
    required_vars = ['DATABRICKS_HOST', 'DATABRICKS_TOKEN']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        print(f"⚠️  Missing required environment variables: {', '.join(missing_vars)}", file=sys.stderr)
        print("Please configure these in your MCP client environment settings", file=sys.stderr)
        print("Tools will fail without valid Databricks credentials", file=sys.stderr)
    else:
        print("✅ Databricks credentials configured", file=sys.stderr)
    
    # Load configuration
    config = load_config()
    servername = config.get('servername', 'databricks-mcp')
    
    print(f"🚀 Starting MCP server: {servername}", file=sys.stderr)
    print(f"🔗 Databricks workspace: {os.environ.get('DATABRICKS_HOST', 'Not set')}", file=sys.stderr)
    print(f"📡 Transport: stdio", file=sys.stderr)
    print("", file=sys.stderr)
    
    # Create MCP server
    mcp_server = FastMCP(name=servername)
    
    # Load prompts and tools
    load_prompts(mcp_server)
    load_tools(mcp_server)
    
    # Run with stdio transport
    try:
        print("🎯 MCP server ready, waiting for initialize...", file=sys.stderr)
        mcp_server.run()
    except KeyboardInterrupt:
        print("👋 MCP server stopped", file=sys.stderr)
    except Exception as e:
        print(f"❌ Error running MCP server: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()

#!/bin/bash
set -e

echo "=========================================="
echo "Mocking Goose - Local Development Setup"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if running on macOS or Linux
OS_TYPE=$(uname -s)
echo "Detected OS: $OS_TYPE"
echo ""

# 1. Check Python version
echo "Checking Python version..."
PYTHON_CMD=""
if command -v python3.13 &> /dev/null; then
    PYTHON_CMD="python3.13"
elif command -v python3.12 &> /dev/null; then
    PYTHON_CMD="python3.12"
elif command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
    if [[ "$PYTHON_VERSION" == "3.13" ]] || [[ "$PYTHON_VERSION" == "3.12" ]]; then
        PYTHON_CMD="python3"
    fi
fi

if [ -z "$PYTHON_CMD" ]; then
    echo -e "${RED}Error: Python 3.12 or 3.13 is required but not found.${NC}"
    echo "Please install Python 3.12 or 3.13 first:"
    echo "  - macOS: brew install python@3.13"
    echo "  - Ubuntu/Debian: sudo apt install python3.13"
    exit 1
fi

echo -e "${GREEN}✓ Found Python: $($PYTHON_CMD --version)${NC}"
echo ""

# 2. Install uv (Python package manager)
echo "Installing uv package manager..."
if ! command -v uv &> /dev/null; then
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # Add uv to PATH for this session
    export PATH="$HOME/.local/bin:$PATH"

    echo -e "${GREEN}✓ uv installed successfully${NC}"
else
    echo -e "${GREEN}✓ uv already installed${NC}"
fi
echo ""

# 3. Install Goose
echo "Installing Goose AI assistant..."
if ! command -v goose &> /dev/null && [ ! -f "$HOME/.local/bin/goose" ]; then
    echo "Installing Goose..."

    # Unset Databricks OAuth credentials to avoid conflicts
    unset DATABRICKS_CLIENT_ID
    unset DATABRICKS_CLIENT_SECRET

    curl -fsSL https://github.com/block/goose/releases/download/stable/download_cli.sh | \
        CONFIGURE=false GOOSE_BIN_DIR="$HOME/.local/bin" bash

    echo -e "${GREEN}✓ Goose installed successfully${NC}"
else
    echo -e "${GREEN}✓ Goose already installed${NC}"
fi
echo ""

# 4. Create local config directory
echo "Creating local configuration directory..."
mkdir -p "$HOME/.config/goose-local"
echo -e "${GREEN}✓ Config directory created${NC}"
echo ""

# 5. Install Python dependencies
echo "Installing Python dependencies..."
if [ -f "requirements.txt" ]; then
    $PYTHON_CMD -m pip install --upgrade pip
    $PYTHON_CMD -m pip install -r requirements.txt
    echo -e "${GREEN}✓ Main dependencies installed${NC}"
else
    echo -e "${YELLOW}⚠ requirements.txt not found${NC}"
fi
echo ""

# 6. Setup mock-and-roll
echo "Setting up mock-and-roll..."
if [ -d "mock-and-roll" ]; then
    cd mock-and-roll

    # Use uv to pin Python version and sync
    uv python pin 3.13 || uv python pin 3.12
    uv sync

    cd ..
    echo -e "${GREEN}✓ mock-and-roll setup complete${NC}"
else
    echo -e "${RED}✗ mock-and-roll directory not found${NC}"
fi
echo ""

# 7. Setup awesome-databricks-mcp
echo "Setting up awesome-databricks-mcp..."
if [ -d "awesome-databricks-mcp" ]; then
    cd awesome-databricks-mcp

    # Use uv to pin Python version and sync
    uv python pin 3.13 || uv python pin 3.12
    uv sync

    cd ..
    echo -e "${GREEN}✓ awesome-databricks-mcp setup complete${NC}"
else
    echo -e "${YELLOW}⚠ awesome-databricks-mcp directory not found${NC}"
fi
echo ""

# 8. Create .env.local if it doesn't exist
if [ ! -f ".env.local" ]; then
    echo "Creating .env.local from template..."
    cp .env.local.example .env.local

    # Update paths in .env.local
    CURRENT_DIR=$(pwd)
    if [ "$OS_TYPE" == "Darwin" ]; then
        sed -i '' "s|\${PWD}|$CURRENT_DIR|g" .env.local
        sed -i '' "s|\${HOME}|$HOME|g" .env.local
    else
        sed -i "s|\${PWD}|$CURRENT_DIR|g" .env.local
        sed -i "s|\${HOME}|$HOME|g" .env.local
    fi

    echo -e "${GREEN}✓ .env.local created${NC}"
    echo -e "${YELLOW}⚠ Please edit .env.local if you need to customize paths${NC}"
else
    echo -e "${GREEN}✓ .env.local already exists${NC}"
fi
echo ""

# 9. Make run script executable
if [ -f "run_local.sh" ]; then
    chmod +x run_local.sh
    echo -e "${GREEN}✓ run_local.sh is now executable${NC}"
fi
echo ""

echo "=========================================="
echo -e "${GREEN}Setup Complete!${NC}"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Review and edit .env.local if needed"
echo "  2. Run: ./run_local.sh"
echo "  3. Open: http://localhost:8000"
echo "  4. Enter your Databricks credentials in the web UI"
echo ""
echo "To start the app later, just run: ./run_local.sh"
echo ""

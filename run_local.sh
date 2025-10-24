#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Mocking Goose - Local Development"
echo "=========================================="
echo ""

# Check if setup has been run
if [ ! -f ".env.local" ]; then
    echo -e "${RED}Error: .env.local not found!${NC}"
    echo ""
    echo "Please run the setup script first:"
    echo "  ./setup_local.sh"
    echo ""
    exit 1
fi

# Check if uv is installed
if ! command -v uv &> /dev/null && [ ! -f "$HOME/.local/bin/uv" ]; then
    echo -e "${RED}Error: uv not found!${NC}"
    echo ""
    echo "Please run the setup script first:"
    echo "  ./setup_local.sh"
    echo ""
    exit 1
fi

# Check if goose is installed
if ! command -v goose &> /dev/null && [ ! -f "$HOME/.local/bin/goose" ]; then
    echo -e "${RED}Error: goose not found!${NC}"
    echo ""
    echo "Please run the setup script first:"
    echo "  ./setup_local.sh"
    echo ""
    exit 1
fi

# Load .env.local
if [ -f ".env.local" ]; then
    echo "Loading environment from .env.local..."
    set -a  # automatically export all variables
    source <(grep -v '^#' .env.local | grep -v '^$' | sed 's/#.*//')
    set +a  # stop automatically exporting
    echo -e "${GREEN}✓ Environment loaded${NC}"
fi

echo ""
echo "Configuration:"
echo "  Mock-and-Roll: ${LOCAL_MOCK_AND_ROLL_DIR:-./mock-and-roll}"
echo "  Goose Bin Dir: ${GOOSE_BIN_DIR:-~/.local/bin}"
echo "  App Host: ${APP_HOST:-0.0.0.0}"
echo "  App Port: ${APP_PORT:-8000}"
echo ""

# Find Python
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
    echo -e "${RED}Error: Python 3.12 or 3.13 not found${NC}"
    exit 1
fi

echo -e "${GREEN}Starting Mocking Goose...${NC}"
echo ""
echo "Access the app at: http://${APP_HOST:-0.0.0.0}:${APP_PORT:-8000}"
echo ""
echo "Press Ctrl+C to stop"
echo ""
echo "=========================================="
echo ""

# Run the local version of the app
exec $PYTHON_CMD proxy_app_local.py

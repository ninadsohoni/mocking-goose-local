"""
Core module for AI-Native Data Demo Framework

This module provides a simplified import interface for core functionality needed
by the sales demo, allowing users to import essential functions directly from 
the core module.

Usage:
    from core import *
"""

# Data Models
from .data import Dataset, DataModel

# Logging Configuration
from .logging_config import setup_logging, get_logger

# CLI Argument Parsing
from .cli import parse_demo_args

# Define what gets exported when using "from core import *"
__all__ = [
    # Data Models
    'Dataset',
    'DataModel',
    
    # Logging
    'setup_logging',
    'get_logger',
    
    # CLI
    'parse_demo_args',
]
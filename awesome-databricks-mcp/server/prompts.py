"""MCP Prompts loader - SIMPLE implementation per CLAUDE.md."""

import glob
import os
import re

import yaml


def load_prompts(mcp_server):
  """Load prompts from markdown files with MCP metadata.

  Parses YAML frontmatter for MCP configuration and registers prompts dynamically.
  """
  import sys
  from pathlib import Path
  
  # Use absolute path to prompts directory
  script_dir = Path(__file__).parent.parent
  prompts_dir = script_dir / 'prompts'
  
  prompt_files = list(prompts_dir.glob('*.md'))

  for prompt_file in prompt_files:
    # Parse the markdown file for metadata and content
    metadata, content = parse_prompt_file(str(prompt_file))

    if not metadata:
      # Skip files without YAML frontmatter
      prompt_name = prompt_file.stem
      print(f'Warning: Skipping {prompt_name} - no YAML frontmatter found', file=sys.stderr)
      continue

    # Register both as prompt (for future MCP clients) and as tool (for current goose compatibility)
    register_mcp_prompt(mcp_server, metadata, content)
    register_prompt_as_tool(mcp_server, metadata, content)


def parse_prompt_file(filepath):
  """Parse markdown file for YAML frontmatter and content."""
  with open(filepath, 'r') as f:
    raw_content = f.read()

  # Check for YAML frontmatter (between --- markers)
  frontmatter_match = re.match(r'^---\s*\n(.*?)\n---\s*\n(.*)$', raw_content, re.DOTALL)

  if frontmatter_match:
    try:
      metadata = yaml.safe_load(frontmatter_match.group(1))
      content = frontmatter_match.group(2)
      return metadata, content
    except yaml.YAMLError as e:
      print(f'Error parsing YAML in {filepath}: {e}')
      return None, raw_content

  return None, raw_content


def register_mcp_prompt(mcp_server, metadata, content):
  """Register prompt with MCP metadata support.

  Note: FastMCP doesn't support argument validation in prompts yet,
  so we return the content with placeholder substitution but without
  runtime validation. The YAML metadata documents the expected arguments
  for future MCP compliance.
  """
  name = metadata.get('name', 'unnamed_prompt')
  description = metadata.get('description', '')
  arguments = metadata.get('arguments', [])

  # Store metadata for documentation (even though FastMCP doesn't use it yet)
  # This prepares for future MCP compliance when FastMCP adds argument support

  # Create a closure that captures the variables correctly
  def create_prompt_handler(prompt_content, prompt_arguments, prompt_name):
    @mcp_server.prompt(name=prompt_name, description=description)
    async def handle_prompt():
      # Build comprehensive documentation including arguments
      text = prompt_content

      # Add argument documentation to the prompt
      if prompt_arguments:
        arg_docs = '\n\n## Expected Arguments:\n'
        for arg in prompt_arguments:
          required = ' (required)' if arg.get('required') else ' (optional)'
          arg_docs += f'- **{arg["name"]}**{required}: {arg.get("description", "No description")}\n'
        text = text + arg_docs

      return [{'role': 'user', 'content': {'type': 'text', 'text': text}}]
    
    return handle_prompt

  # Create the handler with proper variable capture
  create_prompt_handler(content, arguments, name)
  
  import sys
  print(f'✅ Registered MCP prompt: {name} with {len(arguments)} arguments', file=sys.stderr)


def register_prompt_as_tool(mcp_server, metadata, content):
  """Register prompt as an MCP tool for goose compatibility.
  
  This allows goose to access prompts as callable tools until
  goose adds native MCP prompts support.
  """
  name = metadata.get('name', 'unnamed_prompt')
  description = metadata.get('description', '')
  arguments = metadata.get('arguments', [])
  
  # Create tool name with prefix to distinguish from regular tools
  tool_name = f"get_prompt_{name}"
  tool_description = f"Get the '{name}' prompt template. {description}"
  
  # Create a closure that captures the variables correctly
  def create_prompt_tool(prompt_content, prompt_arguments, prompt_name, prompt_description):
    
    @mcp_server.tool(name=tool_name, description=tool_description)
    def get_prompt() -> dict:
      """Get prompt template content."""
      # Build full prompt content with argument documentation
      full_content = prompt_content
      
      if prompt_arguments:
        arg_docs = '\n\n## Required Arguments:\n'
        for arg in prompt_arguments:
          required = ' *(required)*' if arg.get('required') else ' *(optional)*'
          arg_docs += f'- **{arg["name"]}**{required}: {arg.get("description", "No description")}\n'
        full_content = full_content + arg_docs
      
      return {
        "prompt_name": prompt_name,
        "description": prompt_description,
        "content": full_content,
        "arguments": prompt_arguments,
        "usage": f"This is a prompt template for {prompt_description.lower() if prompt_description else 'databricks operations'}."
      }
    
    return get_prompt
  
  # Create the tool with proper variable capture
  create_prompt_tool(content, arguments, name, description)
  
  import sys
  print(f'✅ Registered prompt tool: {tool_name}', file=sys.stderr)


# Note: No fallback support - all prompts must have YAML frontmatter
# This ensures consistency and proper validation across all prompts

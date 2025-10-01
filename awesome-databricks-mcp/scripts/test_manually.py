import asyncio
from fastmcp.client.transports import StdioTransport
from fastmcp import Client


transport = StdioTransport(
    command="uv",
    args=["run", "run_mcp_stdio.py"],
    env={
        "DATABRICKS_HOST": ???,
        "DATABRICKS_TOKEN": ???
    }
)

client = Client(transport)

async def main(client):
    # async with client:
    #     tools = await client.list_tools()
    # return tools
    call_tool_configs = [
        "upload_lakeview_dashboard", {"dashboard_name": "scente_group_dash", "dashboard_file_path": "/Users/maaz.rahman/Documents/repos/mocking-goose/lol.lvdash.json"}
    ]
    print(call_tool_configs)
    async with client:
        result = await client.call_tool(*call_tool_configs)
    print(result)

asyncio.run(main(client))


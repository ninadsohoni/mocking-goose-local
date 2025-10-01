# Mocking Goose - AI-Powered Databricks Demo Platform

A comprehensive Databricks application that combines AI-driven synthetic data generation with interactive demo capabilities. This platform enables solution architects to create compelling, realistic demonstrations using AI-generated datasets and automated pipeline creation.

## 🎯 What is Mocking Goose?

Mocking Goose is a Databricks App that provides:

- **AI-Generated Synthetic Data**: Create realistic, industry-specific datasets using the Mimesis library
- **Automated Pipeline Creation**: Build bronze → silver → gold data pipelines with AI assistance
- **Interactive Demo Interface**: Web-based UI for creating and managing demonstrations
- **MCP Integration**: 63 Databricks tools accessible via Model Context Protocol
- **Session Management**: Per-user backend instances with automatic cleanup

## 🏗️ Architecture

The project consists of three main components:

### 1. **Proxy App** (`proxy_app.py`)
- FastAPI-based web server that manages user sessions
- Proxies requests to individual Goose instances
- Handles authentication and session lifecycle
- Provides web UI for demo creation

### 2. **Mock and Roll** (`mock-and-roll/`)
- AI-native demo framework for Databricks
- Synthetic data generation using Mimesis
- Pre-built utilities for Spark, I/O, and catalog management
- Extensible architecture for custom demos

### 3. **Awesome Databricks MCP** (`awesome-databricks-mcp/`)
- MCP server with 63 Databricks tools
- SQL operations, Unity Catalog management, Jobs, Pipelines
- 2 prompt templates for LDP and Lakeview dashboard creation
- Direct integration with AI assistants

## 🚀 Quick Start

### Prerequisites
- Databricks workspace with Personal Access Token
- [Goose](https://github.com/block/goose) AI assistant

### Deployment to Databricks Apps

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd mocking-goose-main-alm
   ```

2. **Deploy as Databricks App**
   - Upload this directory to your Databricks workspace
   - Create a new Databricks App and point it to this directory as the source
   - The app will automatically handle dependencies and configuration

3. **Access the Application**
   - Launch the app from your Databricks workspace
   - Enter your Databricks credentials when prompted
   - Start creating AI-powered demos!

## 🎨 Features

### Web Interface
- **Login System**: Secure authentication with Databricks credentials
- **Session Management**: Automatic backend instance creation and cleanup
- **Interactive UI**: User-friendly interface for demo creation
- **Real-time Monitoring**: Health checks and system metrics

### AI-Powered Demo Creation
- **Synthetic Data Generation**: Create realistic datasets for any industry
- **Pipeline Automation**: Generate bronze → silver → gold transformations
- **Lakeview Dashboards**: Automated dashboard creation
- **Custom Demos**: AI-assisted creation of industry-specific scenarios

### MCP Integration
- **63 Databricks Tools**: Complete workspace management capabilities
- **SQL Operations**: Execute queries, manage warehouses
- **Unity Catalog**: Schema, table, and volume management
- **Jobs & Pipelines**: Workflow automation
- **Prompt Templates**: Pre-built prompts for common tasks

## 📊 Example Use Cases

### Retail Analytics Demo
```bash
"Create a retail analytics demo with customer behavior data, 
inventory management, and sales forecasting dashboards"
```

### Healthcare Data Pipeline
```bash
"Generate a healthcare demo with patient records, 
clinical trial data, and compliance reporting"
```

### Manufacturing IoT
```bash
"Build a manufacturing demo with sensor data, 
predictive maintenance, and quality control metrics"
```

## 🔧 Configuration

The application supports layered configuration:

1. **Environment Variables**: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`
2. **Configuration Files**: `config/base.yml`, `config/environments/`
3. **Runtime Overrides**: CLI arguments and web interface settings

## 🛠️ Development

### Project Structure
```
mocking-goose-main-alm/
├── proxy_app.py              # Main FastAPI application
├── app.yaml                  # Databricks app configuration
├── mock-and-roll/            # Synthetic data framework
│   ├── src/core/             # Core utilities
│   ├── src/examples/         # Demo templates
│   └── tests/                # Test suite
├── awesome-databricks-mcp/   # MCP server
│   ├── server/tools/         # 63 Databricks tools
│   └── prompts/              # AI prompt templates
└── ui-static/                # Web UI assets
```

### Key Dependencies
- **FastAPI**: Web framework and API
- **uvicorn**: ASGI server
- **httpx**: HTTP client for proxying
- **websockets**: Real-time communication
- **mimesis**: Synthetic data generation
- **databricks-connect**: Databricks integration
- **fastmcp**: MCP server implementation

## 🚀 Deployment

### Databricks Apps Deployment
1. **Upload to Workspace**: Clone this repository into your Databricks workspace
2. **Create Databricks App**: Point a Databricks App to this directory as its source
3. **Configure Environment**: The app automatically handles dependencies and configuration
4. **Launch**: Deploy and access via the Databricks Apps interface

The application is designed to run seamlessly within the Databricks environment with automatic dependency management and configuration.

## 📈 Monitoring

The application provides comprehensive monitoring:

- **Health Endpoint**: `/_health` for system status
- **Session Metrics**: Active sessions and resource usage
- **Process Monitoring**: CPU, memory, and thread statistics
- **WebSocket Connections**: Real-time connection tracking

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the terms specified in the individual component licenses.

## 🙏 Acknowledgments

- **Mock and Roll**: AI-native demo framework by Xavier Armitage
- **Awesome Databricks MCP**: MCP server by Pulkit Chadha
- **Goose**: AI coding assistant by Block
- **Created by**: Jeremy Herbert, Maaz Rahman & Xavier Armitage

---

**Transform your Databricks presentations with AI-generated synthetic data pipelines and interactive demos.**

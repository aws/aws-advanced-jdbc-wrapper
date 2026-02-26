# AWS Advanced JDBC Wrapper MCP Server

This repository includes a Model Context Protocol (MCP) server implementation for the AWS Advanced JDBC Wrapper project. The MCP server provides additional context and tools for Amazon Q to assist with development, testing, and usage of the AWS Advanced JDBC Wrapper.

## Overview

The MCP server enables Amazon Q to:

1. Analyze JDBC connection configurations
2. Generate connection strings with appropriate parameters
3. Suggest optimal plugin configurations for different use cases
4. Troubleshoot common connection issues
5. Provide examples for specific scenarios

## Getting Started

### Prerequisites

- Python 3.8 or higher
- AWS Advanced JDBC Wrapper project

### Installation

1. Clone this repository
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

### Running the MCP Server

To start the MCP server:

```bash
python mcp_server.py
```

By default, the server runs on port 8080. You can specify a different port using the `MCP_PORT` environment variable:

```bash
MCP_PORT=8081 python mcp_server.py
```

## Available Tools

### Connection String Generator

Generates JDBC connection strings for different database types with AWS Advanced JDBC Wrapper configuration.

Example usage:
```
generate_connection_string(
    database_type="aurora-postgresql",
    host="my-cluster.cluster-abc123.us-east-1.rds.amazonaws.com",
    database="mydb",
    plugins=["failover", "hostmonitoring"]
)
```

### Plugin Configuration Analyzer

Analyzes and suggests optimal plugin configurations based on the use case.

Example usage:
```
analyze_plugin_configuration(
    use_case="failover",
    database_type="aurora-postgresql"
)
```

### Connection Troubleshooter

Identifies common issues in connection configurations and suggests solutions.

Example usage:
```
troubleshoot_connection(
    connection_string="jdbc:aws-wrapper:postgresql://host:port/database?wrapperPlugins=failover",
    error_message="Connection refused"
)
```

## Integration with Amazon Q

To use the MCP server with Amazon Q, add it as a context provider in your Amazon Q configuration.

## License

This MCP server implementation is released under the Apache 2.0 license, consistent with the AWS Advanced JDBC Wrapper project.

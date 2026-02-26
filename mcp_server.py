#!/usr/bin/env python3
"""
MCP Server for AWS Advanced JDBC Wrapper

This script implements a Model Context Protocol (MCP) server for the AWS Advanced JDBC Wrapper project.
It provides tools for generating connection strings, analyzing plugin configurations, and troubleshooting
connection issues.
"""

import json
import os
import sys
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from connection_generator import ConnectionStringGenerator
from plugin_analyzer import PluginConfigurationAnalyzer
from troubleshooter import ConnectionTroubleshooter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("mcp_server")

# Default port for the MCP server
DEFAULT_PORT = 8080

class MCPRequestHandler(BaseHTTPRequestHandler):
    """Handler for MCP requests."""
    
    def _set_headers(self, content_type="application/json"):
        self.send_response(200)
        self.send_header('Content-type', content_type)
        self.end_headers()
    
    def do_POST(self):
        """Handle POST requests."""
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        request = json.loads(post_data.decode('utf-8'))
        
        logger.info(f"Received request: {request}")
        
        # Process the request based on the path
        if self.path == "/tools":
            response = self.handle_tools_request()
        elif self.path == "/invoke":
            response = self.handle_invoke_request(request)
        else:
            response = {"error": f"Unknown path: {self.path}"}
        
        self._set_headers()
        self.wfile.write(json.dumps(response).encode('utf-8'))
    
    def handle_tools_request(self):
        """Return the list of available tools."""
        return {
            "tools": [
                {
                    "name": "generate_connection_string",
                    "description": "Generates a JDBC connection string for the specified database type with AWS Advanced JDBC Wrapper configuration.",
                    "parameters": {
                        "database_type": {
                            "type": "string",
                            "description": "The type of database (postgresql, mysql, mariadb, aurora-postgresql, aurora-mysql)",
                            "required": True
                        },
                        "host": {
                            "type": "string",
                            "description": "The database host",
                            "required": True
                        },
                        "port": {
                            "type": "integer",
                            "description": "The database port",
                            "required": False
                        },
                        "database": {
                            "type": "string",
                            "description": "The database name",
                            "required": True
                        },
                        "plugins": {
                            "type": "array",
                            "description": "List of plugins to enable (failover, failover2, iam, secretsmanager, hostmonitoring, readwritesplitting, drivermetadata, telemetry, initialauroraconnectionstrategy)",
                            "required": False
                        },
                        "additional_params": {
                            "type": "object",
                            "description": "Additional connection parameters",
                            "required": False
                        }
                    }
                },
                {
                    "name": "analyze_plugin_configuration",
                    "description": "Analyzes and suggests optimal plugin configurations based on the use case.",
                    "parameters": {
                        "use_case": {
                            "type": "string",
                            "description": "The use case (failover, failover2, high-availability, read-write-splitting, authentication, secrets-manager, driver-metadata, efm, multi-az-rds, telemetry, initial-aurora-connection-strategy)",
                            "required": True
                        },
                        "database_type": {
                            "type": "string",
                            "description": "The type of database (postgresql, mysql, mariadb, aurora-postgresql, aurora-mysql)",
                            "required": True
                        },
                        "current_config": {
                            "type": "object",
                            "description": "Current configuration parameters",
                            "required": False
                        }
                    }
                },
                {
                    "name": "troubleshoot_connection",
                    "description": "Identifies common issues in connection configurations and suggests solutions.",
                    "parameters": {
                        "connection_string": {
                            "type": "string",
                            "description": "The JDBC connection string to troubleshoot",
                            "required": True
                        },
                        "error_message": {
                            "type": "string",
                            "description": "The error message received, if any",
                            "required": False
                        }
                    }
                }
            ]
        }
    
    def handle_invoke_request(self, request):
        """Handle tool invocation requests."""
        tool_name = request.get("name")
        parameters = request.get("parameters", {})
        
        if tool_name == "generate_connection_string":
            generator = ConnectionStringGenerator()
            result = generator.generate(
                database_type=parameters.get("database_type"),
                host=parameters.get("host"),
                port=parameters.get("port"),
                database=parameters.get("database"),
                plugins=parameters.get("plugins", []),
                additional_params=parameters.get("additional_params", {})
            )
            return {"result": result}
        
        elif tool_name == "analyze_plugin_configuration":
            analyzer = PluginConfigurationAnalyzer()
            result = analyzer.analyze(
                use_case=parameters.get("use_case"),
                database_type=parameters.get("database_type"),
                current_config=parameters.get("current_config", {})
            )
            return {"result": result}
        
        elif tool_name == "troubleshoot_connection":
            troubleshooter = ConnectionTroubleshooter()
            result = troubleshooter.troubleshoot(
                connection_string=parameters.get("connection_string"),
                error_message=parameters.get("error_message")
            )
            return {"result": result}
        
        else:
            return {"error": f"Unknown tool: {tool_name}"}

def run_server(port=DEFAULT_PORT):
    """Run the MCP server."""
    server_address = ('', port)
    httpd = HTTPServer(server_address, MCPRequestHandler)
    logger.info(f"Starting MCP server on port {port}")
    httpd.serve_forever()

if __name__ == "__main__":
    port = int(os.environ.get("MCP_PORT", DEFAULT_PORT))
    run_server(port)

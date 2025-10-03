import logging
from collections.abc import Sequence
from typing import Annotated, Any
import socket
import asyncio
import json

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config_providers import FusionConfig
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.definitions import ToolDefinition

logger = logging.getLogger(__name__)


async def register_fusion_tools(
    dbt_mcp: FastMCP,
    config_provider: FusionConfig,
    exclude_tools: Sequence[ToolName] = [],
) -> None:

    register_tools(
        dbt_mcp,
        await create_fusion_tool_definitions(config_provider),
        exclude_tools,
    )


def _format_lsp_message(content: dict[str, Any]) -> bytes:
    """Format a message with proper LSP headers"""
    formatted_content = json.dumps(content)
    content_bytes = formatted_content.encode('utf-8')
    content_length = len(content_bytes)

    # LSP message format: Content-Length header + \r\n\r\n + JSON content
    message = f"Content-Length: {content_length}\r\n\r\n".encode('utf-8') + content_bytes
    return message

def parse_lsp_message(buffer: bytes) -> tuple[list[dict[str, Any]], bytes]:
    """Parse LSP messages from buffer, return (messages, remaining_buffer)"""
    messages = []

    while True:
        # Look for Content-Length header
        header_end = buffer.find(b'\r\n\r\n')
        if header_end == -1:
            # No complete header found
            break
        header_section = buffer[:header_end].decode('utf-8')
        content_start = header_end + 4  # Skip \r\n\r\n

        # Parse Content-Length
        content_length = None
        for line in header_section.split('\r\n'):
            if line.startswith('Content-Length:'):
                content_length = int(line.split(':', 1)[1].strip())
                break
        if content_length is None:
            # Invalid header, skip this message
            buffer = buffer[content_start:]
            continue
        # Check if we have the complete message
        if len(buffer) < content_start + content_length:
            # Incomplete message, wait for more data
            break
        # Extract the JSON content
        json_content = buffer[content_start:content_start + content_length]
        try:
            parsed_message = json.loads(json_content.decode('utf-8'))
            messages.append({
                'raw': json_content.decode('utf-8'),
                'parsed': parsed_message
            })
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"Error parsing JSON content: {e}")
            print(f"Content: {json_content}")
        # Move to next message
        buffer = buffer[content_start + content_length:]
    return messages, buffer


class LSPConnection:
    def __init__(self):
        self.server: socket.socket | None = None
        self.port: int | None = None
        self.is_connected: bool = False
        self.dbt_lsp_process: asyncio.Task[None] | None = None
        self.listener_task: asyncio.Task[None] | None = None
        self.message_id_counter: int = 0
        self.connected_clients: dict[str, socket.socket] = {}

    async def connect(self) -> bool:
        try:
            self.server, self.port = self._setup_server()
            self.listener_task = asyncio.create_task(self._setup_listener())
            print("Running dbt-lsp")
            self.dbt_lsp_process = asyncio.create_task(self._run_dbt_lsp())
            self.is_connected = True
            return True
        except Exception as e:
            print(f"Failed to establish LSP connection: {e}")
            await self.disconnect()
            return False

    async def disconnect(self):
        """Clean up LSP connection resources"""
        self.is_connected = False
        if self.listener_task and not self.listener_task.done():
            self.listener_task.cancel()
            try:
                await self.listener_task
            except asyncio.CancelledError:
                pass

        if self.dbt_lsp_process and not self.dbt_lsp_process.done():
            self.dbt_lsp_process.cancel()
            try:
                await self.dbt_lsp_process
            except asyncio.CancelledError:
                pass
        if self.server:
            try:
                self.server.close()
            except Exception:
                pass
            self.server = None
        self.port = None

    async def send_command(self, command: dict[str, Any]) -> bool:
        success_count = 0
        command_with_id = command.copy()
        if 'id' not in command_with_id:
            command_with_id['id'] = self.message_id_counter
            self.message_id_counter += 1
        
        formatted_message = format_lsp_message(command_with_id)
        
        # Send to all connected clients
        clients_to_remove = []
        for client_addr, client_socket in self.connected_clients.items():
            try:
                await asyncio.get_event_loop().sock_sendall(client_socket, formatted_message)
                print(f"Sent command to client {client_addr}: {json.dumps(command_with_id, indent=2)}")
                success_count += 1
            except Exception as e:
                print(f"Failed to send command to client {client_addr}: {e}")
                clients_to_remove.append(client_addr)
        
        # Remove failed clients
        for client_addr in clients_to_remove:
            del self.connected_clients[client_addr]
        
        return success_count > 0

    async def get_column_lineage(self, model_id: str, column_name: str) -> bool:
        selector = f"+column:{model_id}.{column_name.upper()}+"

        command = {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "workspace/executeCommand",
            "params": {
                "command": "dbt.listNodes",
                "arguments": [selector]
                }
            }
        }

        return await self.send_command(command)


    async def wait_for_completion(self):
        """Wait for both LSP process and listener to complete"""
        if self.is_connected and self.dbt_lsp_process and self.listener_task:
            await asyncio.gather(self.dbt_lsp_process, self.listener_task)

    def _setup_server(self):
        """Internal method to set up the server socket"""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.listen(1)
        server_port = server.getsockname()[1]
        print(f"Server listening on port {server_port}")
        return (server, server_port)
    
    async def _setup_listener(self):
        """Internal method to set up the client listener"""
        if not self.server:
            raise RuntimeError("Server not initialized")
        
        print("Setting up listener")
        self.server.setblocking(False)

        while True:
            try:
                client, addr = await asyncio.get_event_loop().sock_accept(self.server)
                client.setblocking(False)
                client_addr = f"{addr[0]}:{addr[1]}"
                self.connected_clients[client_addr] = client
                print(f"Connection from {addr}")
                asyncio.create_task(self._handle_client(client, addr))
            except Exception as e:
                print(f"Error in listener: {e}")
                await asyncio.sleep(0.1)

    async def _handle_client(self, client: socket.socket, addr: tuple[str, int]):
        client_addr = f"{addr[0]}:{addr[1]}"
        try:
            # Send initial message with proper LSP framing
            init_message = {
                "jsonrpc": "2.0", 
                "id": self.message_id_counter, 
                "method": "initialize", 
                "params": {
                    "capabilities": {
                        "textDocument": {
                            "completion": {
                                "completionItem": {
                                    "documentationFormat": ["markdown"]
                                }
                            }
                        }
                    }
                }
            }
            self.message_id_counter += 1
            
            formatted_message = _format_lsp_message(init_message)
            await asyncio.get_event_loop().sock_sendall(client, formatted_message)
            print(f"Sent initialize message to client {addr}: {json.dumps(init_message, indent=2)}")
            
            # Continuously listen for messages from the client
            buffer = b""
            while True:
                try:
                    data = await asyncio.get_event_loop().sock_recv(client, 4096)
                    if not data:
                        print(f"Client {addr} disconnected")
                        break
                    
                    buffer += data
                    
                    # Parse complete LSP messages from buffer
                    messages, buffer = parse_lsp_message(buffer)
                    
                    for message_info in messages:
                        print(f"Received message from client {addr}:")
                        print(f"  Raw: {message_info['raw']}")
                        print(f"  Parsed: {json.dumps(message_info['parsed'], indent=2)}")
                            
                except Exception as e:
                    print(f"Error receiving from client {addr}: {e}")
                    break
                    
        except Exception as e:
            print(f"Error handling client {addr}: {e}")
        finally:
            # Remove from connected clients
            if client_addr in self.connected_clients:
                del self.connected_clients[client_addr]
            
            try:
                client.close()
                print(f"Closed connection to client {addr}")
            except Exception:
                pass
    
    async def _run_dbt_lsp(self):
        """Internal method to run the dbt-lsp process"""
        if not self.port:
            raise RuntimeError("Port not initialized")
        
        print(f"Running dbt-lsp on port {self.port}")
        process = await asyncio.create_subprocess_exec(
            "/Users/alan/Library/Application Support/Code/User/globalStorage/dbtlabsinc.dbt/bin/dbt-lsp",
            "--socket",
            str(self.port),
            "--project-dir",
            "/Users/alan/dev/dbt/copilot_jaffle_shop_testing"
        )
        await process.wait()
        print("Dbt-lsp process completed")


async def create_fusion_tool_definitions(config: FusionConfig) -> list[ToolDefinition]:
    lsp_connection = LSPConnection()

    if await lsp_connection.connect():
        async def get_column_lineage_tool(
            description: Annotated[str, Field(description="Description of the fusion operation")]
        ) -> str:
            """
            Example fusion tool that demonstrates the pattern.
            
            This tool can combine multiple dbt operations into a single workflow.
            """
            try:
                # Placeholder for fusion logic
                result = f"Fusion operation executed: {description}"
                return result
            except Exception as e:
                logger.error(f"Error in fusion example tool: {e}")
                raise ValueError(f"Fusion tool error: {str(e)}")

        return [
            ToolDefinition(
                fn=get_column_lineage_tool,
                description="Get the lineage of a column within a dbt model"
            )
        ]
    else:
        return []


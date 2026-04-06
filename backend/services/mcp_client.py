import json
import asyncio
import subprocess
import threading
import logging
from typing import Optional

logger = logging.getLogger("powerbi-advisor")


class MCPClient:
    """Communicate with the Power BI Modeling MCP server via stdio JSON-RPC.

    Protocol: JSON-RPC 2.0 over stdio, newline-delimited JSON (ndjson).
    The .NET ModelContextProtocol StdioServerTransport reads one JSON object per line.
    Uses subprocess.Popen + threads for Windows compatibility (asyncio subprocess
    pipes don't work reliably under uvicorn's event loop on Windows).
    """

    def __init__(self, exe_path: str):
        self.exe_path = exe_path
        self.process: Optional[subprocess.Popen] = None
        self._id = 0
        self._timeout = 30
        self._stdout_lines: list = []
        self._stdout_event = threading.Event()
        self._reader_thread: Optional[threading.Thread] = None

    async def start(self):
        """Launch the MCP server subprocess and complete the MCP handshake."""
        self.process = subprocess.Popen(
            [self.exe_path, "--start", "--skipconfirmation"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        # Start background thread to read stdout lines
        self._reader_thread = threading.Thread(target=self._stdout_reader, daemon=True)
        self._reader_thread.start()

        # Wait for the server to finish initializing (reads stderr)
        await self._wait_for_startup()

        # MCP handshake
        init_resp = await self._send("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "powerbi-advisor", "version": "1.0"},
        })
        logger.info(f"MCP initialized: {json.dumps(init_resp, default=str)[:300]}")
        await self._send_notification("notifications/initialized", {})

    def _stdout_reader(self):
        """Background thread: read lines from stdout and queue them."""
        try:
            buf = b""
            while self.process and self.process.stdout:
                chunk = self.process.stdout.read(1)
                if not chunk:
                    break
                buf += chunk
                if chunk == b"\n":
                    self._stdout_lines.append(buf.decode("utf-8", errors="replace"))
                    buf = b""
                    self._stdout_event.set()
        except Exception:
            pass

    async def _wait_for_startup(self):
        """Read stderr until the server signals it's ready."""
        loop = asyncio.get_event_loop()
        def _read_stderr():
            while True:
                line = self.process.stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").strip()
                logger.debug(f"MCP startup: {text}")
                if "Application started" in text or "transport reading messages" in text:
                    return
        try:
            await asyncio.wait_for(loop.run_in_executor(None, _read_stderr), timeout=20)
        except asyncio.TimeoutError:
            logger.warning("MCP startup: timed out, proceeding anyway")

    def _next_id(self) -> int:
        self._id += 1
        return self._id

    def _write(self, data: str):
        """Write a line to the MCP server stdin."""
        self.process.stdin.write((data + "\n").encode("utf-8"))
        self.process.stdin.flush()

    async def _send_notification(self, method: str, params: dict):
        msg = json.dumps({"jsonrpc": "2.0", "method": method, "params": params})
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._write, msg)

    async def _send(self, method: str, params: dict) -> dict:
        """Send a JSON-RPC request and wait for the matching response."""
        req_id = self._next_id()
        msg = json.dumps({
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
            "params": params,
        })
        if not self.process or not self.process.stdin:
            raise RuntimeError("MCP server not started")

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._write, msg)

        # Read response(s) — skip notifications until we get our matching response
        while True:
            resp = await asyncio.wait_for(self._read_message(), timeout=self._timeout)
            if not resp:
                continue
            if "id" in resp:
                if resp.get("error"):
                    err = resp["error"]
                    raise RuntimeError(f"MCP error {err.get('code')}: {err.get('message')}")
                return resp.get("result", {})

    async def _read_message(self) -> dict:
        """Read one JSON-RPC message from the stdout queue."""
        loop = asyncio.get_event_loop()
        def _wait_for_line():
            while not self._stdout_lines:
                if not self._stdout_event.wait(timeout=1):
                    if self.process and self.process.poll() is not None:
                        raise RuntimeError("MCP server process exited")
                    continue
                self._stdout_event.clear()
            return self._stdout_lines.pop(0)

        line = await asyncio.wait_for(loop.run_in_executor(None, _wait_for_line), timeout=self._timeout)
        line = line.strip()
        if not line:
            return {}
        return json.loads(line)

    async def call_tool(self, tool_name: str, request: dict) -> dict:
        """Call an MCP tool. All tools take a single 'request' parameter."""
        logger.info(f"MCP call: {tool_name}({json.dumps(request, default=str)[:300]})")
        resp = await self._send("tools/call", {
            "name": tool_name,
            "arguments": {"request": request},
        })
        logger.info(f"MCP response: {json.dumps(resp, default=str)[:300]}")
        # Check for tool-level errors in content
        if resp.get("isError"):
            text = self._extract_text(resp)
            raise RuntimeError(f"MCP tool error: {text}")
        return resp

    def _extract_text(self, resp: dict) -> str:
        """Extract text content from an MCP tool response."""
        content = resp.get("content", [])
        if isinstance(content, list):
            parts = [item.get("text", "") for item in content if isinstance(item, dict)]
            return "\n".join(parts)
        return str(content)

    # ── High-level operations matching microsoft/powerbi-modeling-mcp v0.1.9 ──
    # All tools take {request: {operation, ...}} — verified against actual tool schemas

    async def list_local_instances(self) -> dict:
        """Find running Power BI Desktop instances."""
        return await self.call_tool("connection_operations", {
            "operation": "ListLocalInstances",
        })

    async def connect(self, data_source: str, initial_catalog: str = None) -> dict:
        """Connect to a PBI Desktop instance via localhost:<port>."""
        req = {"operation": "Connect", "dataSource": data_source}
        if initial_catalog:
            req["initialCatalog"] = initial_catalog
        return await self.call_tool("connection_operations", req)

    async def list_connections(self) -> dict:
        return await self.call_tool("connection_operations", {"operation": "ListConnections"})

    async def list_tables(self) -> dict:
        return await self.call_tool("table_operations", {"operation": "List"})

    async def list_relationships(self) -> dict:
        return await self.call_tool("relationship_operations", {"operation": "List"})

    async def list_measures(self) -> dict:
        return await self.call_tool("measure_operations", {"operation": "List"})

    async def create_relationship(self, from_table: str, from_col: str,
                                   to_table: str, to_col: str,
                                   cross_filter: str = "OneDirection") -> dict:
        """Create a single relationship using relationship_operations Create."""
        return await self.call_tool("relationship_operations", {
            "operation": "Create",
            "relationshipDefinition": {
                "fromTable": from_table,
                "fromColumn": from_col,
                "toTable": to_table,
                "toColumn": to_col,
                "crossFilteringBehavior": cross_filter,
                "isActive": True,
            },
        })

    async def create_measure(self, table_name: str, name: str, expression: str,
                              description: str = "") -> dict:
        """Create a single DAX measure using measure_operations Create."""
        req = {
            "operation": "Create",
            "createDefinition": {
                "tableName": table_name,
                "name": name,
                "expression": expression,
            },
        }
        if description:
            req["createDefinition"]["description"] = description
        return await self.call_tool("measure_operations", req)

    async def create_calculated_table(self, table_name: str, dax_expression: str) -> dict:
        """Create a calculated table using a DAX expression."""
        return await self.call_tool("table_operations", {
            "operation": "Create",
            "tableName": table_name,
            "createDefinition": {
                "daxExpression": dax_expression,
            },
        })

    async def execute_dax(self, query: str) -> dict:
        return await self.call_tool("dax_query_operations", {
            "operation": "Execute",
            "query": query,
        })

    async def get_table_columns(self, table_name: str) -> dict:
        """Get full column details for a table."""
        return await self.call_tool("table_operations", {
            "operation": "GetSchema",
            "tableName": table_name,
        })

    async def delete_measure(self, table_name: str, name: str) -> dict:
        return await self.call_tool("measure_operations", {
            "operation": "Delete",
            "measureName": name,
            "tableName": table_name,
            "shouldCascadeDelete": True,
        })

    async def update_measure(self, table_name: str, name: str,
                              expression: str = None, description: str = None) -> dict:
        update_def = {"tableName": table_name, "name": name}
        if expression is not None:
            update_def["expression"] = expression
        if description is not None:
            update_def["description"] = description
        return await self.call_tool("measure_operations", {
            "operation": "Update",
            "updateDefinition": update_def,
        })

    async def delete_table(self, table_name: str) -> dict:
        return await self.call_tool("table_operations", {
            "operation": "Delete",
            "tableName": table_name,
            "shouldCascadeDelete": True,
        })

    async def delete_relationship(self, name: str) -> dict:
        return await self.call_tool("relationship_operations", {
            "operation": "Delete",
            "relationshipName": name,
            "shouldCascadeDelete": True,
        })

    async def get_model(self) -> dict:
        return await self.call_tool("model_operations", {"operation": "Get"})

    async def stop(self):
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            self.process = None

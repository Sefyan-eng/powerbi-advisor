import json
import os
import re
import asyncio
import logging
from typing import Dict

from fastapi import APIRouter, HTTPException

from config import get_client
from models import DeployRequest, PromptRequest
from services.mcp_client import MCPClient
from services.analyzer import parse_json_response

logger = logging.getLogger("powerbi-advisor")

router = APIRouter()


@router.post("/deploy-to-desktop")
async def deploy_to_desktop(req: DeployRequest):
    """Deploy relationships and DAX measures to a running Power BI Desktop instance via MCP.

    Flow: Start MCP -> find PBI Desktop -> connect -> create relationships -> create measures.
    Requires: Power BI Desktop running with the data file loaded (Get Data -> Excel).
    """
    if not os.path.isfile(req.mcp_exe_path):
        raise HTTPException(400, f"MCP server not found at: {req.mcp_exe_path}")

    mcp = MCPClient(req.mcp_exe_path)
    results = {"connected": False, "relationships": [], "measures": [], "errors": []}

    try:
        # 1. Start MCP server and complete protocol handshake
        await mcp.start()
        logger.info("MCP server started and initialized")

        # 2. Find running PBI Desktop instances
        instances_resp = await mcp.list_local_instances()
        instances_text = mcp._extract_text(instances_resp)
        logger.info(f"Local PBI instances: {instances_text[:500]}")

        # Parse the port from ListLocalInstances response (format: "localhost:PORT")
        port_match = re.search(r"localhost:(\d+)", instances_text)
        if not port_match:
            raise RuntimeError(
                f"No running Power BI Desktop instance found. "
                f"Please open Power BI Desktop and load your data file first. "
                f"Server response: {instances_text[:200]}"
            )

        # 3. Connect to the PBI Desktop instance
        pbi_port = port_match.group(0)  # "localhost:12345"
        logger.info(f"Connecting to PBI Desktop at {pbi_port}")
        conn_resp = await mcp.connect(pbi_port)
        conn_text = mcp._extract_text(conn_resp)
        logger.info(f"Connected: {conn_text[:300]}")
        results["connected"] = True
        results["instance"] = pbi_port

        # 4. List existing tables and extract names for validation
        tables_resp = await mcp.list_tables()
        tables_text = mcp._extract_text(tables_resp)
        logger.info(f"Tables in model: {tables_text[:500]}")

        # Parse table names from MCP response
        existing_tables = set()
        try:
            tables_data = json.loads(tables_text)
            for t in tables_data.get("data", []):
                existing_tables.add(t.get("name", ""))
        except (json.JSONDecodeError, TypeError):
            # Try to extract table names from text
            for match in re.findall(r'"name"\s*:\s*"([^"]+)"', tables_text):
                existing_tables.add(match)

        results["pbi_tables"] = sorted(existing_tables)
        logger.info(f"Tables found in PBI Desktop: {existing_tables}")

        # 5. Get source table column schema (needed for validation)
        source_columns_cache: Dict[str, set] = {}
        for tbl_name in list(existing_tables):
            try:
                schema_resp = await mcp.get_table_columns(tbl_name)
                schema_text = mcp._extract_text(schema_resp)
                schema_data = json.loads(schema_text)
                source_columns_cache[tbl_name] = {
                    c["name"] for c in schema_data.get("data", {}).get("Columns", [])
                    if not c.get("isHidden", False)
                }
            except Exception:
                source_columns_cache[tbl_name] = set()

        # Collect ALL columns referenced in relationships — dimensions MUST include these
        rel_columns_needed: Dict[str, set] = {}  # table_name -> {columns needed for relationships}
        for r in req.model.relationships:
            rel_columns_needed.setdefault(r.from_table, set()).add(r.from_column)
            rel_columns_needed.setdefault(r.to_table, set()).add(r.to_column)

        # 6. Create missing tables as DAX calculated tables
        results["tables_created"] = []
        for t in req.model.tables:
            if t.name in existing_tables:
                continue  # Table already exists

            # Find the source table in PBI Desktop
            source = t.source_sheet
            if source not in existing_tables:
                source = next(iter(existing_tables), None)
            if not source:
                results["errors"].append(f"Cannot create table '{t.name}': no source table in PBI Desktop.")
                continue

            src_cols = source_columns_cache.get(source, set())

            # Start with the AI-recommended columns, filtered to real ones
            valid_cols = [c for c in t.columns if c in src_cols]

            # AUTO-ADD: columns needed for relationships that exist in source
            extra_rel_cols = rel_columns_needed.get(t.name, set())
            for rc in extra_rel_cols:
                if rc in src_cols and rc not in valid_cols:
                    valid_cols.append(rc)

            if not valid_cols:
                results["errors"].append(
                    f"Cannot create table '{t.name}': none of its columns exist in '{source}'."
                )
                continue

            # Build DAX expression
            col_refs = ", ".join([f'"{c}", \'{source}\'[{c}]' for c in valid_cols])
            if t.type == "Dimension":
                dax = f"DISTINCT(SELECTCOLUMNS('{source}', {col_refs}))"
            else:
                dax = f"SELECTCOLUMNS('{source}', {col_refs})"

            try:
                resp = await mcp.create_calculated_table(t.name, dax)
                resp_text = mcp._extract_text(resp)
                try:
                    resp_data = json.loads(resp_text)
                    if resp_data.get("success") is False:
                        raise RuntimeError(resp_data.get("message", "Failed"))
                except json.JSONDecodeError:
                    pass
                results["tables_created"].append({
                    "name": t.name, "type": t.type,
                    "columns": valid_cols, "source": source, "status": "created",
                })
                existing_tables.add(t.name)
                source_columns_cache[t.name] = set(valid_cols)
                logger.info(f"Created calculated table: {t.name} ({t.type}) with {len(valid_cols)} cols from {source}")
            except Exception as e:
                results["errors"].append(f"Table {t.name}: {e}")
                logger.error(f"Failed to create table {t.name}: {e}")

        results["pbi_tables"] = sorted(existing_tables)

        # 7. Create relationships — skip if tables/columns don't exist
        for r in req.model.relationships:
            label = f"{r.from_table}[{r.from_column}] -> {r.to_table}[{r.to_column}]"

            # Pre-validate: both tables and columns must exist
            from_cols = source_columns_cache.get(r.from_table, set())
            to_cols = source_columns_cache.get(r.to_table, set())
            if r.from_table not in existing_tables:
                results["errors"].append(f"Relationship {label}: table '{r.from_table}' does not exist, skipped.")
                continue
            if r.to_table not in existing_tables:
                results["errors"].append(f"Relationship {label}: table '{r.to_table}' does not exist, skipped.")
                continue
            if from_cols and r.from_column not in from_cols:
                results["errors"].append(f"Relationship {label}: column '{r.from_column}' not in '{r.from_table}', skipped.")
                continue
            if to_cols and r.to_column not in to_cols:
                results["errors"].append(f"Relationship {label}: column '{r.to_column}' not in '{r.to_table}', skipped.")
                continue

            try:
                cf = "OneDirection" if r.cross_filter == "Single" else "BothDirections"
                resp = await mcp.create_relationship(
                    r.from_table, r.from_column,
                    r.to_table, r.to_column, cf
                )
                resp_text = mcp._extract_text(resp)
                try:
                    resp_data = json.loads(resp_text)
                    if resp_data.get("success") is False:
                        msg = resp_data.get("message", "Unknown error")
                        if "already exists" in msg.lower():
                            results["relationships"].append({"rel": label, "status": "exists"})
                            continue
                        raise RuntimeError(msg)
                except json.JSONDecodeError:
                    pass
                results["relationships"].append({"rel": label, "status": "created"})
                logger.info(f"Created relationship: {label}")
            except Exception as e:
                err_str = str(e)
                if "already exists" in err_str.lower():
                    results["relationships"].append({"rel": label, "status": "exists"})
                else:
                    results["errors"].append(f"Relationship {label}: {e}")
                    logger.error(f"Failed relationship {label}: {e}")

        # 8. Create DAX measures — validate references first
        measure_table = None
        for t in req.model.tables:
            if t.type == "Fact" and t.name in existing_tables:
                measure_table = t.name
                break
        if not measure_table:
            for t in req.model.tables:
                if t.name in existing_tables:
                    measure_table = t.name
                    break
        if not measure_table and existing_tables:
            measure_table = sorted(existing_tables)[0]

        if measure_table:
            # Pre-validate DAX: check Table[Column] references exist
            dax_ref_pattern = re.compile(r"(\w+)\[(\w+)\]")
            for m in req.model.measures_suggested:
                dax = m.dax
                # Check for references to non-existent tables/columns
                refs = dax_ref_pattern.findall(dax)
                bad_refs = []
                for tbl, col in refs:
                    if tbl in source_columns_cache:
                        if col not in source_columns_cache[tbl]:
                            bad_refs.append(f"{tbl}[{col}]")
                    elif tbl not in existing_tables and tbl not in ("SELECTEDVALUE", "SWITCH", "IF"):
                        bad_refs.append(f"{tbl}[{col}] (table not found)")
                if bad_refs:
                    results["errors"].append(
                        f"Measure '{m.name}' skipped — DAX references non-existent columns: {', '.join(bad_refs)}"
                    )
                    continue

                try:
                    resp = await mcp.create_measure(measure_table, m.name, dax, m.description)
                    resp_text = mcp._extract_text(resp)
                    try:
                        resp_data = json.loads(resp_text)
                        if resp_data.get("success") is False:
                            msg = resp_data.get("message", "Unknown error")
                            if "already exists" in msg.lower():
                                results["measures"].append({"name": m.name, "table": measure_table, "status": "exists"})
                                continue
                            raise RuntimeError(msg)
                    except json.JSONDecodeError:
                        pass
                    results["measures"].append({
                        "name": m.name, "table": measure_table, "status": "created",
                    })
                    logger.info(f"Created measure: {m.name}")
                except Exception as e:
                    err_str = str(e)
                    if "already exists" in err_str.lower():
                        results["measures"].append({"name": m.name, "table": measure_table, "status": "exists"})
                    else:
                        results["errors"].append(f"Measure '{m.name}': {e}")
                        logger.error(f"Failed measure {m.name}: {e}")

        # 9. Verify: re-read the model to confirm changes
        verify_measures = await mcp.list_measures()
        verify_text = mcp._extract_text(verify_measures)
        logger.info(f"Post-deploy measures: {verify_text[:500]}")
        try:
            verify_data = json.loads(verify_text)
            results["verified_measures"] = verify_data.get("data", [])
        except (json.JSONDecodeError, TypeError):
            pass

        verify_rels = await mcp.list_relationships()
        verify_rels_text = mcp._extract_text(verify_rels)
        logger.info(f"Post-deploy relationships: {verify_rels_text[:500]}")
        try:
            verify_rels_data = json.loads(verify_rels_text)
            results["verified_relationships"] = verify_rels_data.get("data", [])
        except (json.JSONDecodeError, TypeError):
            pass

        results["success"] = len(results["errors"]) == 0

    except Exception as e:
        err_msg = str(e) or repr(e)
        # Capture stderr from MCP process for diagnostics
        if mcp.process and mcp.process.stderr:
            try:
                stderr_bytes = await asyncio.wait_for(mcp.process.stderr.read(4096), timeout=2)
                stderr_text = stderr_bytes.decode("utf-8", errors="replace").strip()
                if stderr_text:
                    err_msg += f" | MCP stderr: {stderr_text[:500]}"
            except Exception:
                pass
        logger.error(f"MCP deploy failed: {err_msg}", exc_info=True)
        results["success"] = False
        results["errors"].append(err_msg)
    finally:
        await mcp.stop()

    return results


@router.post("/prompt-model")
async def prompt_model(req: PromptRequest):
    """Natural language interface to modify Power BI Desktop model via MCP.

    Flow: connect to PBI Desktop -> read current model state -> send to Claude
    with user prompt -> Claude returns structured actions -> execute via MCP.
    """
    if not os.path.isfile(req.mcp_exe_path):
        raise HTTPException(400, f"MCP server not found at: {req.mcp_exe_path}")

    mcp = MCPClient(req.mcp_exe_path)
    results = {"actions": [], "errors": [], "reply": "", "model_state": None}

    try:
        # 1. Start MCP and connect to PBI Desktop
        await mcp.start()

        instances_resp = await mcp.list_local_instances()
        instances_text = mcp._extract_text(instances_resp)
        port_match = re.search(r"localhost:(\d+)", instances_text)
        if not port_match:
            raise RuntimeError("No running Power BI Desktop instance found. Open PBI Desktop and load your data first.")

        await mcp.connect(port_match.group(0))

        # 2. Read current model state (tables + columns for each table)
        tables_resp = await mcp.list_tables()
        tables_text = mcp._extract_text(tables_resp)

        # Get column details for each table
        table_details = []
        try:
            tables_data = json.loads(tables_text)
            for t in tables_data.get("data", []):
                tname = t.get("name", "")
                if tname:
                    try:
                        schema_resp = await mcp.get_table_columns(tname)
                        schema_text = mcp._extract_text(schema_resp)
                        table_details.append(f"Table '{tname}': {schema_text}")
                    except Exception:
                        table_details.append(f"Table '{tname}': columns unknown")
        except (json.JSONDecodeError, TypeError):
            table_details.append(f"Tables raw: {tables_text}")

        rels_resp = await mcp.list_relationships()
        rels_text = mcp._extract_text(rels_resp)

        measures_resp = await mcp.list_measures()
        measures_text = mcp._extract_text(measures_resp)

        model_state = (
            f"TABLES AND COLUMNS:\n" + "\n".join(table_details) +
            f"\n\nRELATIONSHIPS:\n{rels_text}\n\nMEASURES:\n{measures_text}"
        )
        results["model_state"] = model_state[:3000]

        # 3. Ask Claude to plan operations
        system_prompt = f"""Tu es un expert Power BI connecté en direct à un modèle sémantique Power BI Desktop via MCP.
Voici l'état actuel du modèle :

{model_state}

L'utilisateur veut modifier ce modèle. Analyse sa demande et réponds en JSON valide (sans markdown).

Actions disponibles :

{{
  "reply": "Explication courte en français de ce que tu fais",
  "actions": [
    {{"type": "create_measure", "table": "Table", "name": "Nom", "expression": "DAX", "description": "desc"}},
    {{"type": "delete_measure", "table": "Table", "name": "NomMesure"}},
    {{"type": "update_measure", "table": "Table", "name": "NomMesure", "expression": "NEW DAX", "description": "new desc"}},
    {{"type": "create_relationship", "from_table": "T1", "from_column": "c1", "to_table": "T2", "to_column": "c2", "cross_filter": "OneDirection"}},
    {{"type": "delete_relationship", "name": "RelationshipName"}},
    {{"type": "create_table", "name": "NomTable", "dax_expression": "SELECTCOLUMNS(...)"}},
    {{"type": "delete_table", "name": "NomTable"}},
    {{"type": "execute_dax", "query": "EVALUATE ..."}},
    {{"type": "info", "message": "Information ou conseil"}}
  ]
}}

Règles :
- Utilise UNIQUEMENT les tables et colonnes qui EXISTENT dans le modèle ci-dessus
- Pour les mesures DAX, utilise la syntaxe DAX correcte et valide
- N'invente JAMAIS de colonnes ou tables qui n'existent pas
- Pour supprimer, utilise delete_measure/delete_table/delete_relationship
- Pour modifier une mesure existante, utilise update_measure
- Pour créer une table calculée, utilise create_table avec une expression DAX (SELECTCOLUMNS, DISTINCT, etc.)
- Si la demande n'est pas claire, retourne un "info" avec une question de clarification
- Réponds UNIQUEMENT en JSON valide, pas de markdown"""

        messages = []
        for msg in req.conversation:
            messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
        messages.append({"role": "user", "content": req.prompt})

        client = get_client()
        claude_resp = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=4096,
            system=system_prompt,
            messages=messages,
        )

        try:
            plan = parse_json_response(claude_resp.content[0].text)
        except Exception:
            results["reply"] = claude_resp.content[0].text
            results["success"] = True
            return results

        results["reply"] = plan.get("reply", "")
        actions = plan.get("actions", [])

        # 4. Execute planned actions
        for action in actions:
            action_type = action.get("type")
            try:
                if action_type == "create_measure":
                    await mcp.create_measure(
                        action["table"], action["name"],
                        action["expression"], action.get("description", "")
                    )
                    results["actions"].append({
                        "type": "create_measure",
                        "name": action["name"],
                        "table": action["table"],
                        "status": "done",
                    })

                elif action_type == "delete_measure":
                    await mcp.delete_measure(action["table"], action["name"])
                    results["actions"].append({
                        "type": "delete_measure",
                        "name": action["name"],
                        "status": "done",
                    })

                elif action_type == "update_measure":
                    await mcp.update_measure(
                        action["table"], action["name"],
                        action.get("expression"), action.get("description"),
                    )
                    results["actions"].append({
                        "type": "update_measure",
                        "name": action["name"],
                        "status": "done",
                    })

                elif action_type == "create_relationship":
                    await mcp.create_relationship(
                        action["from_table"], action["from_column"],
                        action["to_table"], action["to_column"],
                        action.get("cross_filter", "OneDirection"),
                    )
                    results["actions"].append({
                        "type": "create_relationship",
                        "rel": f"{action['from_table']}[{action['from_column']}] -> {action['to_table']}[{action['to_column']}]",
                        "status": "done",
                    })

                elif action_type == "delete_relationship":
                    await mcp.delete_relationship(action["name"])
                    results["actions"].append({
                        "type": "delete_relationship",
                        "name": action["name"],
                        "status": "done",
                    })

                elif action_type == "create_table":
                    await mcp.create_calculated_table(action["name"], action["dax_expression"])
                    results["actions"].append({
                        "type": "create_table",
                        "name": action["name"],
                        "status": "done",
                    })

                elif action_type == "delete_table":
                    await mcp.delete_table(action["name"])
                    results["actions"].append({
                        "type": "delete_table",
                        "name": action["name"],
                        "status": "done",
                    })

                elif action_type == "execute_dax":
                    dax_resp = await mcp.execute_dax(action["query"])
                    dax_text = mcp._extract_text(dax_resp)
                    results["actions"].append({
                        "type": "execute_dax",
                        "query": action["query"],
                        "result": dax_text[:1000],
                        "status": "done",
                    })

                elif action_type == "info":
                    results["actions"].append({
                        "type": "info",
                        "message": action.get("message", ""),
                        "status": "done",
                    })

            except Exception as e:
                results["actions"].append({
                    "type": action_type,
                    "status": "error",
                    "error": str(e),
                })
                results["errors"].append(f"{action_type}: {e}")

        results["success"] = len(results["errors"]) == 0

    except Exception as e:
        err_msg = str(e) or repr(e)
        logger.error(f"Prompt-model failed: {err_msg}", exc_info=True)
        results["success"] = False
        results["errors"].append(err_msg)
    finally:
        await mcp.stop()

    return results

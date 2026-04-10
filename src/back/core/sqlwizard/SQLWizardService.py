"""
SQL Wizard Service - Text-to-SQL Generation

Provides LLM-based SQL generation from natural language prompts,
with schema context discovery and SQL validation.
"""
import re
import hashlib
from typing import Dict, List, Any, Optional, Tuple

from back.core.logging import get_logger
from back.core.errors import ValidationError, InfrastructureError

from .models import SchemaContext

logger = get_logger(__name__)


class SQLWizardService:
    """Service for generating SQL from natural language using LLM endpoints."""
    
    # SQL keywords that indicate non-SELECT statements
    FORBIDDEN_KEYWORDS = [
        'CREATE', 'ALTER', 'DROP', 'INSERT', 'UPDATE', 'DELETE', 
        'TRUNCATE', 'GRANT', 'REVOKE', 'MERGE', 'REPLACE'
    ]
    
    # Default query limit
    DEFAULT_LIMIT = 100
    
    def __init__(self, databricks_client):
        """Initialize the wizard service with a Databricks client."""
        self.client = databricks_client
        self._schema_cache: Dict[str, SchemaContext] = {}
    
    def get_model_serving_endpoints(self) -> List[Dict[str, str]]:
        """Get list of text-capable model serving endpoints from the workspace.
        
        Returns:
            List of dicts with 'name' and 'state' keys
        """
        import requests
        
        if not self.client.host or not self.client.has_valid_auth():
            return []
        
        try:
            host = self.client.host.rstrip('/')
            headers = self.client._get_auth_headers()
            
            # Use the serving endpoints API
            url = f"{host}/api/2.0/serving-endpoints"
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            endpoints = []
            for ep in data.get('endpoints', []):
                # Filter for endpoints that can handle text/chat completions
                # Look for foundation models, external models, or custom models
                endpoint_type = ep.get('config', {}).get('served_entities', [{}])[0].get('entity_name', '')
                
                endpoints.append({
                    'name': ep.get('name', ''),
                    'state': ep.get('state', {}).get('ready', 'UNKNOWN'),
                    'endpoint_type': endpoint_type
                })
            
            logger.info(f"[SQLWizard] Found {len(endpoints)} serving endpoints")
            return endpoints
            
        except Exception as e:
            logger.error(f"[SQLWizard] Error fetching serving endpoints: {e}")
            return []
    
    def get_schema_context(self, catalog: str, schema: str, use_cache: bool = True) -> SchemaContext:
        """Collect schema metadata for the specified catalog and schema.
        
        Note: This method is deprecated. Prefer using pre-built schema context with full_name in each table.
        
        Args:
            catalog: Unity Catalog catalog name
            schema: Schema name
            use_cache: Whether to use cached schema context
            
        Returns:
            SchemaContext with tables and columns
        """
        cache_key = f"{catalog}.{schema}"
        
        if use_cache and cache_key in self._schema_cache:
            logger.info(f"[SQLWizard] Using cached schema context for {cache_key}")
            return self._schema_cache[cache_key]
        
        # Get tables in the schema
        tables = self.client.get_tables(catalog, schema)
        
        table_metadata = []
        for table_name in tables:
            try:
                columns = self.client.get_table_columns(catalog, schema, table_name)
                full_name = f"{catalog}.{schema}.{table_name}"
                table_metadata.append({
                    'name': table_name,
                    'full_name': full_name,
                    'columns': columns
                })
            except Exception as e:
                logger.warning(f"[SQLWizard] Could not get columns for {table_name}: {e}")
                full_name = f"{catalog}.{schema}.{table_name}"
                table_metadata.append({'name': table_name, 'full_name': full_name, 'columns': []})
        
        context = SchemaContext(tables=table_metadata)
        
        # Cache the result
        self._schema_cache[cache_key] = context
        logger.info(f"[SQLWizard] Built schema context for {cache_key}: {len(tables)} tables")
        
        return context
    
    def compose_prompt(self, user_prompt: str, schema_context: SchemaContext, limit: int = None, mapping_type: str = None) -> Dict[str, str]:
        """Compose the full prompt for the LLM.
        
        Args:
            user_prompt: User's natural language request
            schema_context: Schema context object
            limit: Query result limit (default: DEFAULT_LIMIT). Ignored for entity/relationship mapping types.
            mapping_type: Type of mapping ('entity', 'relationship', or None for general)
            
        Returns:
            Dict with 'system' and 'user' messages
        """
        logger.info("[SQLWizard] compose_prompt: mapping_type=%s, prompt=%d chars", mapping_type, len(user_prompt))
        if limit is None:
            limit = self.DEFAULT_LIMIT
        
        is_mapping = mapping_type in ('entity', 'relationship')
        
        # Base system instruction (general / ad-hoc queries only)
        system_instruction = (
            "You are a text-to-SQL assistant for Databricks SQL. "
            "Produce ONLY a single SELECT statement valid for Databricks SQL. "
            "No DDL/DML. Use only the provided tables/columns. "
            "Prefer explicit aliases. "
            f"Always include LIMIT {limit} unless the user explicitly asks otherwise."
        )
        
        # Add mapping-type-specific instructions
        if mapping_type == 'entity':
            system_instruction = (
                "You are a text-to-SQL assistant for Databricks SQL, specialized for entity data extraction. "
                "Produce ONLY a single SELECT statement valid for Databricks SQL. "
                "No DDL/DML. Use only the provided tables/columns. "
                "CRITICAL RULES for entity queries:\n"
                "1. The FIRST column MUST be aliased AS ID (e.g. customer_id AS ID).\n"
                "2. The SECOND column MUST be aliased AS Label (e.g. customer_name AS Label).\n"
                "3. Every column name in the SELECT list MUST be unique. "
                "If the same underlying column is needed both as an alias and as an attribute, "
                "include it twice — once with the alias, once with its original name "
                "(e.g. SELECT cust_id AS ID, cust_name AS Label, cust_id, cust_name, …).\n"
                "4. Write a simple flat SELECT from a single table when possible.\n"
                "5. Do NOT wrap the query in subqueries or CTEs.\n"
                "6. Do NOT add ORDER BY clauses.\n"
                "7. Add WHERE IS NOT NULL on the identifier column to filter out null keys.\n"
                "8. Do NOT add any LIMIT clause — the query must return ALL rows."
            )
        elif mapping_type == 'relationship':
            system_instruction = (
                "You are a text-to-SQL assistant for Databricks SQL, specialized for relationship/link data extraction. "
                "Produce ONLY a single SELECT statement valid for Databricks SQL. "
                "No DDL/DML. Use only the provided tables/columns. "
                "CRITICAL RULES for relationship queries:\n"
                "1. SELECT ONLY the two identifier columns needed for the relationship.\n"
                "2. The first column MUST be aliased AS source_id.\n"
                "3. The second column MUST be aliased AS target_id.\n"
                "4. Every column name in the SELECT list MUST be unique.\n"
                "5. If both identifier columns are in the SAME table, query only that ONE table (no joins needed).\n"
                "6. Do NOT add ORDER BY clauses.\n"
                "7. Keep the query as simple as possible - minimal joins, no unnecessary complexity.\n"
                "8. Do NOT add any LIMIT clause — the query must return ALL rows."
            )
        
        rendered_schema = schema_context.to_yaml_like()
        
        # Build user message
        if mapping_type == 'entity':
            user_message = (
                f"Available objects:\n{rendered_schema}\n\n"
                f"Task: {user_prompt}\n\n"
                f"Constraints:\n"
                f"- The FIRST column MUST be aliased AS ID\n"
                f"- The SECOND column MUST be aliased AS Label\n"
                f"- Every column name in the SELECT list must be UNIQUE — "
                f"if the same column is needed for an alias and as an attribute, include it twice\n"
                f"- Write a simple flat SELECT (no subqueries, no CTEs, no nested queries)\n"
                f"- Do NOT add ORDER BY\n"
                f"- Do NOT add LIMIT — return all rows"
            )
        elif mapping_type == 'relationship':
            user_message = (
                f"Available objects:\n{rendered_schema}\n\n"
                f"Task: {user_prompt}\n\n"
                f"Constraints:\n"
                f"- SELECT EXACTLY 2 columns: the source identifier AS source_id and the target identifier AS target_id\n"
                f"- Every column name in the SELECT list must be UNIQUE\n"
                f"- If both columns are in the same table, use only that table (no joins)\n"
                f"- Do NOT add ORDER BY\n"
                f"- Do NOT add extra columns beyond the two identifiers\n"
                f"- Do NOT add LIMIT — return all rows"
            )
        else:
            user_message = (
                f"Available objects:\n{rendered_schema}\n\n"
                f"Task: {user_prompt}\n\n"
                f"Constraints: Use only listed objects; ISO date literals; "
                f"avoid full scans; add WHERE predicates where appropriate; LIMIT {limit}."
            )
        
        logger.debug("[SQLWizard] compose_prompt: system (%d chars): %.200s…", len(system_instruction), system_instruction)
        logger.debug("[SQLWizard] compose_prompt: user (%d chars): %.300s…", len(user_message), user_message)
        return {
            'system': system_instruction,
            'user': user_message
        }
    
    def call_llm_endpoint(self, endpoint_name: str, prompt: Dict[str, str], timeout: int = 60) -> str:
        """Call the LLM endpoint and get the generated SQL.
        
        Args:
            endpoint_name: Name of the model serving endpoint
            prompt: Dict with 'system' and 'user' messages
            timeout: Request timeout in seconds
            
        Returns:
            Generated SQL text
        """
        import requests
        import time
        
        if not self.client.host or not self.client.has_valid_auth():
            raise ValidationError("Databricks credentials not configured")
        
        host = self.client.host.rstrip('/')
        headers = self.client._get_auth_headers()
        
        url = f"{host}/serving-endpoints/{endpoint_name}/invocations"
        
        payload = {
            "messages": [
                {"role": "system", "content": prompt['system']},
                {"role": "user", "content": prompt['user']}
            ],
            "max_tokens": 1024,
            "temperature": 0.1
        }
        
        logger.info("[SQLWizard] call_llm_endpoint: POST %s (timeout=%ds)", endpoint_name, timeout)
        logger.debug("[SQLWizard] call_llm_endpoint: url=%s", url)
        logger.debug(
            "[SQLWizard] call_llm_endpoint: payload system=%d chars, user=%d chars",
            len(prompt.get('system', '')), len(prompt.get('user', '')),
        )
        
        try:
            t0 = time.time()
            response = requests.post(url, headers=headers, json=payload, timeout=timeout)
            elapsed_ms = int((time.time() - t0) * 1000)
            logger.info(
                "[SQLWizard] call_llm_endpoint: response status=%d, %d bytes in %dms",
                response.status_code, len(response.content), elapsed_ms,
            )
            logger.debug("[SQLWizard] call_llm_endpoint: response body preview: %.500s", response.text)
            response.raise_for_status()
            
            result = response.json()
            
            # Extract content from response
            if 'choices' in result:
                content = result['choices'][0].get('message', {}).get('content', '')
                logger.debug("[SQLWizard] call_llm_endpoint: extracted from choices — %d chars", len(content))
            elif 'predictions' in result:
                content = result['predictions'][0] if result['predictions'] else ''
                logger.debug("[SQLWizard] call_llm_endpoint: extracted from predictions — %d chars", len(content))
            else:
                content = str(result)
                logger.warning("[SQLWizard] call_llm_endpoint: unexpected response format, keys=%s", list(result.keys()))
            
            # Log token usage if present
            usage = result.get('usage', {})
            if usage:
                logger.info(
                    "[SQLWizard] call_llm_endpoint: tokens prompt=%d, completion=%d, total=%d",
                    usage.get('prompt_tokens', 0), usage.get('completion_tokens', 0), usage.get('total_tokens', 0),
                )
            
            logger.info("[SQLWizard] call_llm_endpoint: LLM returned %d chars of content", len(content))
            return content
            
        except requests.exceptions.Timeout:
            logger.error("[SQLWizard] call_llm_endpoint: timeout after %ds", timeout)
            raise TimeoutError(f"LLM endpoint call timed out after {timeout}s")
        except requests.exceptions.HTTPError as e:
            logger.error(
                "[SQLWizard] call_llm_endpoint: HTTP error status=%s, body=%.300s",
                e.response.status_code if e.response is not None else "?",
                e.response.text[:300] if e.response is not None else "N/A",
            )
            raise InfrastructureError(f"LLM endpoint error: {e.response.text if e.response else str(e)}")
    
    def extract_sql(self, llm_output: str) -> str:
        """Extract a single SQL statement from the LLM output.
        
        Handles code fences, prose, and multiple statements.
        
        Args:
            llm_output: Raw output from the LLM
            
        Returns:
            Cleaned SQL statement
        """
        logger.info("[SQLWizard] extract_sql: input %d chars", len(llm_output))
        logger.debug("[SQLWizard] extract_sql: raw input: %.300s", llm_output)
        text = llm_output.strip()
        
        # Extract from code fences
        fence_pattern = r'```(?:sql)?\s*([\s\S]*?)```'
        matches = re.findall(fence_pattern, text, re.IGNORECASE)
        if matches:
            text = matches[0].strip()
            logger.debug("[SQLWizard] extract_sql: extracted from code fence (%d chars)", len(text))
        
        # If still has prose, extract the SELECT
        if not text.upper().startswith('SELECT'):
            select_pattern = r'(SELECT[\s\S]*?)(?:;|$)'
            match = re.search(select_pattern, text, re.IGNORECASE)
            if match:
                text = match.group(1).strip()
                logger.debug("[SQLWizard] extract_sql: extracted SELECT from prose (%d chars)", len(text))
        
        text = text.rstrip(';').strip()
        
        if ';' in text:
            text = text.split(';')[0].strip()
            logger.debug("[SQLWizard] extract_sql: took first statement before semicolon")
        
        logger.info("[SQLWizard] extract_sql: output %d chars, starts_with_SELECT=%s", len(text), text.upper().startswith('SELECT'))
        logger.debug("[SQLWizard] extract_sql: result: %s", text)
        return text
    
    def validate_sql_static(self, sql: str, schema_context: SchemaContext, limit: int = None, mapping_type: str = None) -> Tuple[bool, str, str]:
        """Perform static validation on the SQL statement.
        
        Args:
            sql: SQL statement to validate
            schema_context: Schema context for table whitelist
            limit: Expected LIMIT value (ignored for entity/relationship mapping types)
            mapping_type: Type of mapping ('entity', 'relationship', or None for general)
            
        Returns:
            Tuple of (is_valid, message, corrected_sql)
        """
        logger.info("[SQLWizard] validate_sql_static: mapping_type=%s, sql=%d chars", mapping_type, len(sql))
        logger.debug("[SQLWizard] validate_sql_static: sql=%s", sql)
        if limit is None:
            limit = self.DEFAULT_LIMIT
        
        is_mapping = mapping_type in ('entity', 'relationship')
        
        corrected_sql = sql
        issues = []
        
        sql_upper = sql.upper().strip()
        if not sql_upper.startswith('SELECT'):
            logger.warning("[SQLWizard] validate_sql_static: not a SELECT statement")
            return False, "Query must be a SELECT statement", sql
        
        # Check for forbidden keywords
        for keyword in self.FORBIDDEN_KEYWORDS:
            pattern = rf'\b{keyword}\b'
            if re.search(pattern, sql_upper):
                logger.warning("[SQLWizard] validate_sql_static: forbidden keyword '%s'", keyword)
                return False, f"Query contains forbidden keyword: {keyword}", sql
        
        # Build table whitelist
        valid_tables = set()
        for table in schema_context.tables:
            table_name = table.get('name', '')
            full_name = table.get('full_name', '')
            if table_name:
                valid_tables.add(table_name.lower())
            if full_name:
                valid_tables.add(full_name.lower())
                parts = full_name.split('.')
                if len(parts) == 3:
                    valid_tables.add(f"{parts[1]}.{parts[2]}".lower())
                    valid_tables.add(parts[2].lower())
        logger.debug("[SQLWizard] validate_sql_static: whitelist has %d table name variants", len(valid_tables))
        
        # Extract table references
        from_pattern = r'\b(?:FROM|JOIN)\s+([^\s,\(\)]+)'
        table_refs = re.findall(from_pattern, sql, re.IGNORECASE)
        logger.debug("[SQLWizard] validate_sql_static: found table refs: %s", table_refs)
        
        for ref in table_refs:
            table_ref = ref.split()[0].strip('`"').lower()
            if table_ref not in valid_tables:
                issues.append(f"Table '{ref}' not in schema context")
        
        if issues:
            logger.warning("[SQLWizard] validate_sql_static: FAILED — %s", "; ".join(issues))
            return False, "; ".join(issues), sql
        
        if is_mapping:
            corrected_sql = re.sub(r'\s+LIMIT\s+\d+\s*$', '', corrected_sql, flags=re.IGNORECASE).strip()
            corrected_sql = self._deduplicate_select_columns(corrected_sql, mapping_type)
            logger.debug("[SQLWizard] validate_sql_static: mapping mode — stripped LIMIT, deduplicated columns")
        else:
            if 'LIMIT' not in sql_upper:
                corrected_sql = f"{sql} LIMIT {limit}"
                logger.info("[SQLWizard] validate_sql_static: added LIMIT %d", limit)
        
        if corrected_sql != sql:
            logger.debug("[SQLWizard] validate_sql_static: corrected SQL: %s", corrected_sql)
        
        logger.info("[SQLWizard] validate_sql_static: PASSED")
        return True, "Validation passed", corrected_sql
    
    # ------------------------------------------------------------------
    # Column deduplication / alias enforcement for mapping queries
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_select_columns(sql: str):
        """Extract the raw SELECT column list string from a SQL statement.
        
        Returns (select_part, rest) where *select_part* is the text between
        SELECT and FROM (respecting parenthesised sub-expressions) and *rest*
        is everything from FROM onwards.
        """
        upper = sql.upper()
        # Skip past SELECT [DISTINCT]
        idx = upper.find('SELECT')
        if idx == -1:
            return None, sql
        start = idx + len('SELECT')
        if upper[start:].lstrip().startswith('DISTINCT'):
            start = upper.index('DISTINCT', start) + len('DISTINCT')

        # Walk forward to find the top-level FROM (not inside parentheses)
        depth = 0
        pos = start
        from_pos = None
        while pos < len(sql):
            ch = sql[pos]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif depth == 0 and upper[pos:pos + 5] == ' FROM' and (pos + 5 >= len(sql) or not sql[pos + 5].isalnum()):
                from_pos = pos
                break
            pos += 1

        if from_pos is None:
            return None, sql

        select_part = sql[start:from_pos].strip()
        rest = sql[from_pos:]
        prefix = sql[:start]  # "SELECT " or "SELECT DISTINCT "
        return (prefix, select_part, rest)

    @staticmethod
    def _split_columns(select_part: str):
        """Split a SELECT column list by commas, respecting parentheses."""
        cols = []
        depth = 0
        current = []
        for ch in select_part:
            if ch == '(':
                depth += 1
                current.append(ch)
            elif ch == ')':
                depth -= 1
                current.append(ch)
            elif ch == ',' and depth == 0:
                cols.append(''.join(current).strip())
                current = []
            else:
                current.append(ch)
        if current:
            cols.append(''.join(current).strip())
        return cols

    @staticmethod
    def _effective_name(col_expr: str) -> str:
        """Return the effective output name of a column expression.
        
        If it has an alias (… AS foo), return the alias.
        Otherwise return the last dot-segment or the whole expression.
        """
        # Check for AS alias (case-insensitive)
        m = re.search(r'\bAS\s+[`"]?(\w+)[`"]?\s*$', col_expr, re.IGNORECASE)
        if m:
            return m.group(1)
        # Bare column — possibly table.column
        token = col_expr.strip().split('.')[-1].strip('`"')
        return token

    def _deduplicate_select_columns(self, sql: str, mapping_type: str = None) -> str:
        """Ensure every column in the SELECT list has a unique output name.
        
        - For *entity* queries: guarantees the first column is aliased AS ID
          and the second AS Label.
        - For *relationship* queries: guarantees the first column is aliased
          AS source_id and the second AS target_id.
        - For any query: if two columns share the same effective output name,
          the later occurrence(s) get a numeric suffix (e.g. col_2).
        """
        parsed = self._parse_select_columns(sql)
        if parsed is None or len(parsed) != 3:
            return sql

        prefix, select_part, rest = parsed
        columns = self._split_columns(select_part)

        if not columns:
            return sql

        # --- Phase 1: enforce mandatory aliases on positional columns ------
        if mapping_type == 'entity':
            required = {0: 'ID', 1: 'Label'}
        elif mapping_type == 'relationship':
            required = {0: 'source_id', 1: 'target_id'}
        else:
            required = {}

        for idx, alias in required.items():
            if idx < len(columns):
                col = columns[idx]
                eff = self._effective_name(col)
                if eff.upper() != alias.upper():
                    # Strip any existing alias first
                    col_stripped = re.sub(r'\s+AS\s+\S+\s*$', '', col, flags=re.IGNORECASE).strip()
                    columns[idx] = f"{col_stripped} AS {alias}"

        # --- Phase 2: deduplicate names ------------------------------------
        seen = {}  # lowercase name -> count
        deduped = []
        for col in columns:
            eff = self._effective_name(col).lower()
            if eff in seen:
                seen[eff] += 1
                new_alias = f"{self._effective_name(col)}_{seen[eff]}"
                col_stripped = re.sub(r'\s+AS\s+\S+\s*$', '', col, flags=re.IGNORECASE).strip()
                deduped.append(f"{col_stripped} AS {new_alias}")
            else:
                seen[eff] = 1
                deduped.append(col)

        new_select = ', '.join(deduped)
        return f"{prefix} {new_select}{rest}"

    def validate_sql_explain(self, sql: str, timeout: int = 30) -> Tuple[bool, str, Optional[Dict]]:
        """Run EXPLAIN on the SQL to validate the query plan.
        
        Args:
            sql: SQL statement to explain
            timeout: Query timeout in seconds
            
        Returns:
            Tuple of (is_valid, message, plan_info)
        """
        try:
            explain_query = f"EXPLAIN {sql}"
            
            # Execute EXPLAIN
            result = self.client.execute_query(explain_query)
            
            # Check for full table scans on large tables
            plan_text = str(result)
            warnings = []
            
            # Look for signs of full table scans
            if 'Scan' in plan_text and 'Filter' not in plan_text:
                warnings.append("Query may perform a full table scan without filters")
            
            plan_info = {
                'plan': result,
                'warnings': warnings
            }
            
            if warnings:
                return True, f"Query is valid but has warnings: {'; '.join(warnings)}", plan_info
            
            return True, "Query plan validated successfully", plan_info
            
        except Exception as e:
            return False, f"EXPLAIN failed: {str(e)}", None
    
    def generate_sql(
        self,
        endpoint_name: str,
        user_prompt: str,
        limit: int = None,
        validate_plan: bool = True,
        schema_context_data: Dict[str, Any] = None,
        mapping_type: str = None,
        catalog: str = None,
        schema: str = None
    ) -> Dict[str, Any]:
        """Full pipeline: generate and validate SQL from natural language.
        
        Args:
            endpoint_name: Model serving endpoint name
            user_prompt: Natural language request
            limit: Query result limit
            validate_plan: Whether to run EXPLAIN validation
            schema_context_data: Pre-built schema context dict with tables (each table has full_name)
            mapping_type: Type of mapping ('entity', 'relationship', or None for general)
            catalog: (deprecated) Unity Catalog catalog - only used if fetching from UC
            schema: (deprecated) Schema name - only used if fetching from UC
            
        Returns:
            Dict with generated SQL, validation results, and telemetry
        """
        if limit is None:
            limit = self.DEFAULT_LIMIT
        
        # Telemetry data (anonymized)
        telemetry = {
            'endpoint': endpoint_name,
            'prompt_hash': hashlib.sha256(user_prompt.encode()).hexdigest()[:16],
            'validation_outcomes': [],
            'mapping_type': mapping_type
        }
        
        try:
            # Step 1: Get schema context
            logger.info("[SQLWizard] generate_sql step 1: resolve schema context")
            if schema_context_data and schema_context_data.get('tables'):
                schema_context = SchemaContext(
                    tables=schema_context_data.get('tables', [])
                )
                logger.info("[SQLWizard] generate_sql: using pre-built schema context — %d table(s)", len(schema_context.tables))
                logger.debug(
                    "[SQLWizard] generate_sql: table names=[%s]",
                    ", ".join(t.get('full_name', t.get('name', '?')) for t in schema_context.tables),
                )
            elif catalog and schema:
                logger.info("[SQLWizard] generate_sql: fetching schema from UC — %s.%s", catalog, schema)
                schema_context = self.get_schema_context(catalog, schema)
            else:
                logger.warning("[SQLWizard] generate_sql: no schema context and no catalog/schema")
                return {
                    'success': False,
                    'error': 'No schema context provided and no catalog/schema specified',
                    'telemetry': telemetry
                }
            telemetry['tables_count'] = len(schema_context.tables)
            
            # Step 2: Compose prompt
            logger.info("[SQLWizard] generate_sql step 2: compose prompt (mapping_type=%s)", mapping_type)
            prompt = self.compose_prompt(user_prompt, schema_context, limit, mapping_type)
            
            # Step 3: Call LLM
            logger.info("[SQLWizard] generate_sql step 3: call LLM endpoint '%s'", endpoint_name)
            raw_output = self.call_llm_endpoint(endpoint_name, prompt)
            logger.debug("[SQLWizard] generate_sql: raw LLM output (%d chars): %.300s", len(raw_output), raw_output)
            
            # Step 4: Extract SQL
            logger.info("[SQLWizard] generate_sql step 4: extract SQL from LLM output")
            sql = self.extract_sql(raw_output)
            
            if not sql:
                logger.warning("[SQLWizard] generate_sql: could not extract SQL from LLM output")
                return {
                    'success': False,
                    'error': 'Could not extract SQL from LLM output',
                    'raw_output': raw_output,
                    'telemetry': telemetry
                }
            logger.info("[SQLWizard] generate_sql: extracted SQL — %d chars", len(sql))
            
            # Step 5: Static validation
            logger.info("[SQLWizard] generate_sql step 5: static validation")
            is_valid, message, corrected_sql = self.validate_sql_static(sql, schema_context, limit, mapping_type)
            telemetry['validation_outcomes'].append({'static': is_valid, 'message': message})
            
            if not is_valid:
                logger.warning("[SQLWizard] generate_sql: static validation FAILED — %s", message)
                return {
                    'success': False,
                    'error': message,
                    'sql': sql,
                    'telemetry': telemetry
                }
            
            # Step 6: Optional EXPLAIN validation
            plan_warning = None
            if validate_plan:
                logger.info("[SQLWizard] generate_sql step 6: EXPLAIN validation")
                plan_valid, plan_message, plan_info = self.validate_sql_explain(corrected_sql)
                telemetry['validation_outcomes'].append({'explain': plan_valid, 'message': plan_message})
                telemetry['explain_status'] = 'success' if plan_valid else 'failed'
                logger.info("[SQLWizard] generate_sql: EXPLAIN result — valid=%s, message=%s", plan_valid, plan_message)
                
                if plan_info and plan_info.get('warnings'):
                    plan_warning = plan_info['warnings'][0]
                    logger.debug("[SQLWizard] generate_sql: EXPLAIN warning: %s", plan_warning)
            else:
                logger.debug("[SQLWizard] generate_sql: EXPLAIN validation skipped")
            
            logger.info("[SQLWizard] generate_sql: SUCCESS — final SQL %d chars", len(corrected_sql))
            logger.debug("[SQLWizard] generate_sql: final SQL: %s", corrected_sql)
            return {
                'success': True,
                'sql': corrected_sql,
                'warning': plan_warning,
                'telemetry': telemetry
            }
            
        except TimeoutError as e:
            logger.error("[SQLWizard] generate_sql: TIMEOUT — %s", e)
            telemetry['validation_outcomes'].append({'error': 'timeout'})
            return {
                'success': False,
                'error': str(e),
                'telemetry': telemetry
            }
        except Exception as e:
            logger.exception("[SQLWizard] generate_sql: FAILED — %s", e)
            telemetry['validation_outcomes'].append({'error': str(e)})
            return {
                'success': False,
                'error': str(e),
                'telemetry': telemetry
            }
    
    def clear_cache(self):
        """Clear the schema context cache."""
        self._schema_cache.clear()
        logger.info("[SQLWizard] Schema cache cleared")

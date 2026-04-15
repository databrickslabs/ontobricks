"""SPARQL query translation to Spark SQL (via R2RML mappings)."""
import re

from back.core.logging import get_logger
from back.core.helpers import sql_escape as _escape_sql, extract_local_name as _extract_local
from back.core.w3c.sparql.constants import DIALECT_SPARK

logger = get_logger(__name__)


class SparqlTranslator:
    # ---------------------------------------------------------------------------
    # SQL dialect helpers (Spark SQL)
    # ---------------------------------------------------------------------------

    @staticmethod
    def _string_type(dialect: str = DIALECT_SPARK) -> str:
        """Return the string type name for the dialect."""
        return "STRING"


    @staticmethod
    def _cast_str(expr: str, dialect: str = DIALECT_SPARK) -> str:
        """CAST(<expr> AS STRING) for Spark SQL."""
        return f"CAST({expr} AS {SparqlTranslator._string_type(dialect)})"


    @staticmethod
    def _coalesce_cast_str(expr: str, dialect: str = DIALECT_SPARK) -> str:
        """COALESCE(CAST(<expr> AS STRING), '')."""
        return f"COALESCE({SparqlTranslator._cast_str(expr, dialect)}, '')"


    @staticmethod
    def _unpivot_select(
        subject_expr: str,
        predicates: list,
        from_clause: str,
        where_clause: str = "",
        dialect: str = DIALECT_SPARK,
        alias: str = "",
    ) -> str:
        """Build a SELECT that unpivots predicate/object pairs using Spark ``stack()``."""
        stack_args = ", ".join([f"'{p[0]}', {p[1]}" for p in predicates])
        return (
            f"SELECT {subject_expr} AS subject, "
            f"stack({len(predicates)}, {stack_args}) AS (predicate, object)\n"
            f"                FROM {from_clause}{where_clause}"
        )


    @staticmethod
    def _subject_expr_from_template(
        uri_template: str,
        id_column: str,
        alias: str = "",
        dialect: str = DIALECT_SPARK,
    ) -> str:
        """Build a subject expression from a URI template, like CONCAT(base, CAST(col AS STRING))."""
        col_ref = f"{alias}.{id_column}" if alias else id_column
        if uri_template and "{" in uri_template:
            base_uri = uri_template.split("{")[0]
            return f"CONCAT('{base_uri}', {SparqlTranslator._cast_str(col_ref, dialect)})"
        return SparqlTranslator._cast_str(col_ref, dialect)


    @staticmethod
    def _collect_source_ctes(mappings, relationship_mappings=None):
        """Deduplicate source table references into CTE definitions.

        Scans entity and relationship mappings, groups identical ``sql_query``
        (or bare ``table``) strings, and returns:

        * **cte_defs** -- ordered list of ``("cte_alias", "sql_text")`` tuples
          ready to be emitted inside a ``WITH`` block.
        * **source_alias** -- dict mapping each original source key to its CTE
          alias so callers can replace inlined subqueries with the short name.

        Source keys are the raw ``sql_query`` string or the ``table`` name.
        """
        seen: dict[str, str] = {}
        cte_defs: list[tuple[str, str]] = []
        counter = 0

        def _register(source_key: str) -> str:
            nonlocal counter
            if source_key in seen:
                return seen[source_key]
            alias = f"src_{counter}"
            counter += 1
            seen[source_key] = alias
            cte_defs.append((alias, source_key))
            return alias

        for _class_uri, mapping in (mappings or {}).items():
            sql_query = (mapping.get('sql_query') or '').strip()
            table = mapping.get('table')
            if sql_query:
                _register(sql_query)
            elif table:
                _register(table)

        for rel in (relationship_mappings or []):
            sql_query = (rel.get('sql_query') or '').strip()
            if sql_query:
                _register(sql_query)

        return cte_defs, seen


    @staticmethod
    def _source_from_alias(source_key: str, source_alias: dict, row_alias: str = "") -> str:
        """Return a CTE alias reference for *source_key*, or fall back to inline."""
        cte = source_alias.get(source_key)
        if cte:
            return f"{cte} AS {row_alias}" if row_alias else cte
        if "SELECT" in source_key.upper():
            return f"({source_key}) AS {row_alias}" if row_alias else f"({source_key})"
        return f"{source_key} AS {row_alias}" if row_alias else source_key


    @staticmethod
    def _build_with_clause(cte_defs):
        """Render the ``WITH ...`` preamble from a list of (alias, sql) tuples."""
        if not cte_defs:
            return ""
        parts = []
        for alias, sql_text in cte_defs:
            if "SELECT" in sql_text.upper():
                parts.append(f"  {alias} AS (\n    {sql_text}\n  )")
            else:
                parts.append(f"  {alias} AS (\n    SELECT * FROM {sql_text}\n  )")
        return "WITH\n" + ",\n".join(parts) + "\n"


    @staticmethod
    def translate_sparql_to_spark(sparql_query, entity_mappings, limit, relationship_mappings=None, dialect=DIALECT_SPARK):
        """Translate SPARQL query to SQL (Spark or PostgreSQL) using R2RML mappings.
        
        Args:
            sparql_query: SPARQL query string
            entity_mappings: Entity mappings from R2RML
            limit: Maximum results
            relationship_mappings: Relationship mappings from R2RML
            dialect: SQL dialect — 'spark' (default) or 'postgres'
            
        Returns:
            dict: Translation result with SQL and variables
        """
        # Normalize query
        query = ' '.join(sparql_query.split())
        
        # Extract prefixes
        prefixes = SparqlTranslator._extract_prefixes(query)
        
        # Remove prefixes from query body
        prefix_pattern = r'PREFIX\s+(\w+):\s*<([^>]+)>'
        query_body = re.sub(prefix_pattern, '', query, flags=re.IGNORECASE).strip()
        
        # Check if SELECT query
        if not re.search(r'\bSELECT\b', query_body, re.IGNORECASE):
            return {'success': False, 'message': 'Only SELECT queries are supported for Spark SQL engine.'}
        
        # Extract SELECT variables
        select_match = re.search(r'SELECT\s+(DISTINCT\s+)?((?:\?\w+\s*)+|\*)', query_body, re.IGNORECASE)
        if not select_match:
            return {'success': False, 'message': 'Could not parse SELECT clause.'}
        
        is_distinct = bool(select_match.group(1))
        select_vars_str = select_match.group(2).strip()
        
        if select_vars_str == '*':
            select_vars = None
        else:
            select_vars = [v.strip()[1:] for v in select_vars_str.split() if v.strip().startswith('?')]
        
        # Extract WHERE clause
        where_match = re.search(r'WHERE\s*\{(.+)\}', query_body, re.IGNORECASE | re.DOTALL)
        if not where_match:
            return {'success': False, 'message': 'Could not parse WHERE clause.'}
        
        where_content = where_match.group(1).strip()
        
        # Parse triple patterns - handle nested OPTIONAL blocks
        optional_blocks, main_content = SparqlTranslator._extract_optional_blocks(where_content)
        logger.debug("Extracted %s OPTIONAL blocks", len(optional_blocks))
        for i, block in enumerate(optional_blocks):
            logger.debug("OPTIONAL block %s: %s...", i, block[:100])
        
        # Clean up main content
        main_content = re.sub(r'\s+', ' ', main_content).strip()
        logger.debug("Main content after removing OPTIONAL blocks: %s...", main_content[:200])
        
        patterns = SparqlTranslator._parse_triple_patterns(main_content, prefixes)
        logger.debug("Main patterns: %s", len(patterns))
        
        optional_patterns = []
        for opt_block in optional_blocks:
            # Parse the content inside OPTIONAL, flattening nested OPTIONALs
            parsed = SparqlTranslator._parse_triple_patterns(opt_block, prefixes)
            logger.debug("Parsed %s patterns from OPTIONAL block", len(parsed))
            for p in parsed:
                logger.debug("  Pattern: %s %s %s", p['subject'], p['predicate'], p['object'])
            optional_patterns.extend(parsed)
        
        if not patterns:
            return {'success': False, 'message': 'Could not parse any triple patterns.'}
        
        # Parse BIND statements from both main and optional content
        bind_values = SparqlTranslator._parse_bind_statements(main_content)
        for opt_block in optional_blocks:
            bind_values.update(SparqlTranslator._parse_bind_statements(opt_block))
        
        # Parse predicate FILTER if present
        predicate_filter = SparqlTranslator._parse_predicate_filter(where_content, prefixes)
        
        # Parse value FILTER clauses (CONTAINS, EQUALS, etc.)
        value_filters = SparqlTranslator._parse_value_filters(where_content, prefixes)
        
        # Parse relationship filters from UNION blocks (BIND patterns)
        relationship_filter = SparqlTranslator._parse_relationship_filters(where_content, prefixes)
        
        # Build SQL from patterns
        return SparqlTranslator._build_spark_sql(patterns, optional_patterns, entity_mappings, select_vars, is_distinct, limit, relationship_mappings, bind_values, predicate_filter, value_filters, relationship_filter, dialect=dialect)


    @staticmethod
    def _extract_optional_blocks(content):
        """Extract OPTIONAL blocks, handling nested braces.
        
        Returns tuple: (blocks, main_content_without_optionals)
        """
        blocks = []
        ranges_to_remove = []  # Track (start, end) ranges to remove
        i = 0
        content_upper = content.upper()
        
        while i < len(content):
            # Find OPTIONAL keyword
            opt_pos = content_upper.find('OPTIONAL', i)
            if opt_pos == -1:
                break
            
            # Find the opening brace
            brace_start = content.find('{', opt_pos)
            if brace_start == -1:
                break
            
            # Count braces to find matching closing brace
            brace_count = 1
            j = brace_start + 1
            while j < len(content) and brace_count > 0:
                if content[j] == '{':
                    brace_count += 1
                elif content[j] == '}':
                    brace_count -= 1
                j += 1
            
            if brace_count == 0:
                # Extract the content between braces
                block_content = content[brace_start + 1:j - 1].strip()
                blocks.append(block_content)
                # Track the full range to remove (from OPTIONAL to closing brace)
                ranges_to_remove.append((opt_pos, j))
                i = j
            else:
                i = opt_pos + 8
        
        # Remove OPTIONAL blocks from content to get main content
        # Process in reverse order to preserve positions
        main_content = content
        for start, end in reversed(ranges_to_remove):
            main_content = main_content[:start] + main_content[end:]
        
        return blocks, main_content.strip()


    @staticmethod
    def _extract_prefixes(query):
        """Extract PREFIX declarations from SPARQL query."""
        prefixes = {
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
            'owl': 'http://www.w3.org/2002/07/owl#',
            'xsd': 'http://www.w3.org/2001/XMLSchema#',
        }
        
        # Match PREFIX declarations, including empty prefix (PREFIX : <uri>)
        prefix_pattern = r'PREFIX\s+(\w*):\s*<([^>]+)>'
        for match in re.finditer(prefix_pattern, query, re.IGNORECASE):
            prefixes[match.group(1)] = match.group(2)
        
        return prefixes


    @staticmethod
    def _parse_triple_patterns(content, prefixes):
        """Parse triple patterns from SPARQL WHERE content."""
        patterns = []
        
        # term matches: ?variable, <uri>, prefix:local (including empty prefix like :local), "literal"
        term = r'(\?\w+|<[^>]+>|\w*:\w+|"[^"]*")'
        # pred matches: ?variable, <uri>, prefix:local, 'a' (shorthand for rdf:type)
        pred = r'(\?\w+|<[^>]+>|\w*:\w+|a)'
        triple_pattern = rf'{term}\s+{pred}\s+{term}'
        
        for match in re.finditer(triple_pattern, content):
            subj, pred_str, obj = match.groups()
            
            subj_resolved = SparqlTranslator._resolve_term(subj, prefixes)
            pred_resolved = SparqlTranslator._resolve_term(pred_str, prefixes)
            obj_resolved = SparqlTranslator._resolve_term(obj, prefixes)
            
            if pred_str == 'a':
                pred_resolved = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
            
            patterns.append({
                'subject': subj_resolved,
                'predicate': pred_resolved,
                'object': obj_resolved,
                'subject_is_var': subj.startswith('?'),
                'predicate_is_var': pred_str.startswith('?'),
                'object_is_var': obj.startswith('?'),
                'subject_var': subj[1:] if subj.startswith('?') else None,
                'predicate_var': pred_str[1:] if pred_str.startswith('?') else None,
                'object_var': obj[1:] if obj.startswith('?') else None,
            })
        
        return patterns


    @staticmethod
    def _resolve_term(term, prefixes):
        """Resolve a SPARQL term to a full URI."""
        if term.startswith('?'):
            return term
        elif term.startswith('<') and term.endswith('>'):
            return term[1:-1]
        elif term.startswith('"'):
            return term
        elif ':' in term:
            prefix, local = term.split(':', 1)
            # Handle empty prefix (default namespace like :Person)
            if prefix == '' and '' in prefixes:
                return prefixes[''] + local
            elif prefix in prefixes:
                return prefixes[prefix] + local
        return term


    @staticmethod
    def _parse_bind_statements(content):
        """Parse BIND statements from SPARQL content.
        
        Returns dict mapping variable name (without ?) to literal value.
        """
        bindings = {}
        # Match BIND("value" AS ?variable) or BIND('value' AS ?variable)
        bind_pattern = r'BIND\s*\(\s*["\']([^"\']+)["\']\s+AS\s+\?(\w+)\s*\)'
        
        for match in re.finditer(bind_pattern, content, re.IGNORECASE):
            value, var_name = match.groups()
            bindings[var_name] = value
            logger.debug("Found BIND: %s = '%s'", var_name, value)
        
        return bindings


    @staticmethod
    def _parse_predicate_filter(content, prefixes):
        """Parse FILTER clauses that restrict predicates.
        
        Matches patterns like: FILTER(?predicate IN (rdf:type, :FirstName, rdfs:label))
        Returns: list of predicate URIs, or None if no filter found
        
        Note: Collects predicates from ALL FILTER clauses (for UNION queries with multiple entity types)
        """
        # Match FILTER(?var IN (...))
        filter_pattern = r'FILTER\s*\(\s*\?(\w+)\s+IN\s*\(\s*([^)]+)\s*\)\s*\)'
        
        # Collect predicates from ALL matching FILTER clauses
        all_predicate_uris = []
        seen_predicates = set()
        
        for match in re.finditer(filter_pattern, content, re.IGNORECASE):
            var_name, predicates_str = match.groups()
            
            # Check if this is filtering the predicate variable
            if var_name.lower() in ['predicate', 'p', 'pred']:
                # Parse the predicate list
                # Split by comma and clean up
                for pred in predicates_str.split(','):
                    pred = pred.strip()
                    if pred:
                        # Resolve the predicate term
                        resolved = SparqlTranslator._resolve_term(pred, prefixes)
                        # Avoid duplicates
                        if resolved not in seen_predicates:
                            all_predicate_uris.append(resolved)
                            seen_predicates.add(resolved)
                            logger.debug("Predicate filter: %s -> %s", pred, resolved)
        
        if all_predicate_uris:
            logger.debug("Found predicate filter with %s predicates total", len(all_predicate_uris))
            return all_predicate_uris
        
        return None


    @staticmethod
    def _parse_relationship_filters(content, prefixes):
        """Parse relationship predicates from UNION blocks.
        
        Matches patterns like:
        - ?subject :WorksWith ?target .
        - BIND(:WorksWith AS ?predicate)
        
        Returns: list of relationship URIs, or None if no relationships found
        """
        relationship_uris = []
        
        # Pattern 1: Direct relationship pattern: ?subject :RelName ?target
        # followed by BIND(:RelName AS ?predicate)
        rel_pattern = r'\?(\w+)\s+(\w*:\w+|<[^>]+>)\s+\?(\w+)\s*\.\s*\n\s*BIND\s*\(\s*(\2)\s+AS\s+\?\w+\s*\)'
        
        for match in re.finditer(rel_pattern, content, re.IGNORECASE):
            pred_term = match.group(2)
            resolved = SparqlTranslator._resolve_term(pred_term, prefixes)
            if resolved not in relationship_uris:
                relationship_uris.append(resolved)
                logger.debug("Found relationship filter (pattern 1): %s -> %s", pred_term, resolved)
        
        # Pattern 2: Simple relationship pattern with BIND
        bind_pattern = r'BIND\s*\(\s*(\w*:\w+|<[^>]+>)\s+AS\s+\?predicate\s*\)'
        for match in re.finditer(bind_pattern, content, re.IGNORECASE):
            pred_term = match.group(1)
            resolved = SparqlTranslator._resolve_term(pred_term, prefixes)
            if resolved not in relationship_uris:
                relationship_uris.append(resolved)
                logger.debug("Found relationship filter (BIND): %s -> %s", pred_term, resolved)
        
        if relationship_uris:
            logger.debug("Found %s relationship filters", len(relationship_uris))
            return relationship_uris
        
        return None


    @staticmethod
    def _parse_value_filters(content, prefixes):
        """Parse FILTER clauses that filter attribute values.
        
        Matches patterns like:
        - FILTER(CONTAINS(LCASE(STR(?var)), "value"))
        - FILTER(STR(?var) = "value")
        - FILTER(STRSTARTS(LCASE(STR(?var)), "value"))
        - FILTER(?var > value)
        
        Returns: list of dicts with {predicate_uri, operator, value, column_var}
        """
        value_filters = []
        
        # First, find all triple patterns that bind attribute values to variables
        # Pattern: ?subject :AttributeName ?varName
        attr_pattern = r'\?(\w+)\s+(\w*:\w+|<[^>]+>)\s+\?(\w+)'
        attr_bindings = {}  # Maps variable names to predicate URIs
        
        for match in re.finditer(attr_pattern, content):
            subject_var, pred_term, obj_var = match.groups()
            resolved_pred = SparqlTranslator._resolve_term(pred_term, prefixes)
            attr_bindings[obj_var] = resolved_pred
            logger.debug("Value filter: found binding ?%s <- %s", obj_var, resolved_pred)
        
        # Now find FILTER clauses that reference these variables
        # CONTAINS pattern
        contains_pattern = r'FILTER\s*\(\s*CONTAINS\s*\(\s*LCASE\s*\(\s*STR\s*\(\s*\?(\w+)\s*\)\s*\)\s*,\s*["\']([^"\']+)["\']\s*\)\s*\)'
        for match in re.finditer(contains_pattern, content, re.IGNORECASE):
            var_name, value = match.groups()
            if var_name in attr_bindings:
                value_filters.append({
                    'predicate_uri': attr_bindings[var_name],
                    'operator': 'contains',
                    'value': value.lower(),
                    'variable': var_name
                })
                logger.debug("Value filter: CONTAINS on %s = '%s'", attr_bindings[var_name], value)
        
        # EQUALS pattern
        equals_pattern = r'FILTER\s*\(\s*STR\s*\(\s*\?(\w+)\s*\)\s*=\s*["\']([^"\']+)["\']\s*\)'
        for match in re.finditer(equals_pattern, content, re.IGNORECASE):
            var_name, value = match.groups()
            if var_name in attr_bindings:
                value_filters.append({
                    'predicate_uri': attr_bindings[var_name],
                    'operator': 'equals',
                    'value': value,
                    'variable': var_name
                })
                logger.debug("Value filter: EQUALS on %s = '%s'", attr_bindings[var_name], value)
        
        # STRSTARTS pattern
        starts_pattern = r'FILTER\s*\(\s*STRSTARTS\s*\(\s*LCASE\s*\(\s*STR\s*\(\s*\?(\w+)\s*\)\s*\)\s*,\s*["\']([^"\']+)["\']\s*\)\s*\)'
        for match in re.finditer(starts_pattern, content, re.IGNORECASE):
            var_name, value = match.groups()
            if var_name in attr_bindings:
                value_filters.append({
                    'predicate_uri': attr_bindings[var_name],
                    'operator': 'starts',
                    'value': value.lower(),
                    'variable': var_name
                })
                logger.debug("Value filter: STRSTARTS on %s = '%s'", attr_bindings[var_name], value)
        
        # STRENDS pattern
        ends_pattern = r'FILTER\s*\(\s*STRENDS\s*\(\s*LCASE\s*\(\s*STR\s*\(\s*\?(\w+)\s*\)\s*\)\s*,\s*["\']([^"\']+)["\']\s*\)\s*\)'
        for match in re.finditer(ends_pattern, content, re.IGNORECASE):
            var_name, value = match.groups()
            if var_name in attr_bindings:
                value_filters.append({
                    'predicate_uri': attr_bindings[var_name],
                    'operator': 'ends',
                    'value': value.lower(),
                    'variable': var_name
                })
                logger.debug("Value filter: STRENDS on %s = '%s'", attr_bindings[var_name], value)
        
        if value_filters:
            logger.debug("Found %s value filters", len(value_filters))
        
        return value_filters if value_filters else None


    @staticmethod
    def _is_generic_triple_pattern(patterns, select_vars):
        """Check if the query is a generic triple pattern."""
        if len(patterns) != 1:
            return False
        
        pattern = patterns[0]
        return (pattern['subject_is_var'] and 
                pattern['predicate_is_var'] and 
                pattern['object_is_var'])


    @staticmethod
    def _is_type_constrained_triple_pattern(patterns):
        """Check if query has type constraint(s) + generic triple pattern.
        
        Pattern: ?s rdf:type <Entity> . ?s ?p ?o .
        Returns: list of class URIs if matched, None otherwise
        """
        rdf_type = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
        
        type_patterns = []
        generic_pattern = None
        subject_var = None
        
        logger.debug("_is_type_constrained_triple_pattern: checking %s patterns", len(patterns))
        
        for pattern in patterns:
            pred_str = pattern['predicate'][:50] if len(pattern['predicate']) > 50 else pattern['predicate']
            obj_str = pattern['object'][:50] if len(pattern['object']) > 50 else pattern['object']
            logger.debug("  Pattern: subj=%s pred=%s obj=%s", pattern['subject'], pred_str, obj_str)
            
            # Check for rdf:type pattern with concrete class
            if (pattern['predicate'] == rdf_type and 
                not pattern['object_is_var'] and 
                pattern['subject_is_var']):
                type_patterns.append(pattern)
                logger.debug("  -> Type pattern detected: %s", pattern['object'])
                if subject_var is None:
                    subject_var = pattern['subject_var']
            
            # Check for generic triple pattern (?s ?p ?o)
            elif (pattern['subject_is_var'] and 
                  pattern['predicate_is_var'] and 
                  pattern['object_is_var']):
                generic_pattern = pattern
                logger.debug("  -> Generic pattern detected: ?%s ?%s ?%s", pattern['subject_var'], pattern['predicate_var'], pattern['object_var'])
        
        # Valid if we have at least one type pattern and one generic pattern
        # and they share the same subject variable
        if type_patterns and generic_pattern:
            if all(tp['subject_var'] == generic_pattern['subject_var'] for tp in type_patterns):
                class_uris = [tp['object'] for tp in type_patterns]
                logger.debug("Type-constrained pattern MATCHED: %s", class_uris)
                return class_uris
        
        logger.debug("Type-constrained pattern NOT matched (type_patterns=%s, generic=%s)", len(type_patterns), generic_pattern is not None)
        return None


    @staticmethod
    def _build_filtered_with_relationships_query(mappings, select_vars, is_distinct, limit, relationship_mappings, filter_class_uris, predicate_filter, value_filters, relationship_filter, dialect=DIALECT_SPARK):
        """Build a CTE-based query for value filters with relationships.
        
        This returns:
        1. For filtered entity type: Only entities matching the value filter
        2. For other entity types: ALL entities (no filter applied)
        3. Relationships: Only relationships where at least one end is a filtered entity
        """
        logger.debug("Building CTE-based filtered query with relationships")

        def get_local_name(uri):
            return _extract_local(uri).lower()

        # Build filter set for classes
        filter_local_names = set(get_local_name(uri) for uri in filter_class_uris) if filter_class_uris else None
        
        # Build helper to get subject expression from template
        def get_subject_expr(uri_template, id_column, alias="e"):
            return SparqlTranslator._subject_expr_from_template(uri_template, id_column, alias, dialect)
        
        # Identify which entity types have value filters applied
        filtered_entity_types = set()
        for vf in value_filters:
            # The predicate_uri in a value filter tells us which entity type it applies to
            # We need to find which entity has this predicate
            pred_uri = vf['predicate_uri']
            for class_uri, mapping in (mappings or {}).items():
                pred_to_column = {}
                label_column = mapping.get('label_column', '')
                if label_column:
                    pred_to_column['http://www.w3.org/2000/01/rdf-schema#label'] = label_column
                for p_uri, p_info in mapping.get('predicates', {}).items():
                    if p_info.get('type') == 'column':
                        pred_to_column[p_uri] = p_info['column']
                        pred_to_column[get_local_name(p_uri)] = p_info['column']
                if pred_uri in pred_to_column or get_local_name(pred_uri) in pred_to_column:
                    filtered_entity_types.add(class_uri)
        
        logger.debug("Entity types with filters: %s", filtered_entity_types)

        # Collect CTEs to deduplicate source table scans
        cte_defs, source_alias = SparqlTranslator._collect_source_ctes(mappings, relationship_mappings)
        
        # Step 1: Build seed_entities CTE (entities matching the value filter)
        seed_unions = []
        for class_uri, mapping in (mappings or {}).items():
            if filter_class_uris is not None:
                if class_uri not in filter_class_uris and get_local_name(class_uri) not in filter_local_names:
                    continue
            
            if class_uri not in filtered_entity_types:
                continue
            
            table = mapping.get('table')
            sql_query = (mapping.get('sql_query') or '').strip()
            source_key = sql_query or table
            if not source_key:
                continue
            from_clause = SparqlTranslator._source_from_alias(source_key, source_alias)
            
            id_column = mapping.get('id_column', 'id')
            uri_template = mapping.get('uri_template', '')
            subject_expr = get_subject_expr(uri_template, id_column, "e")
            
            # Build WHERE conditions from value filters
            pred_to_column = {}
            label_column = mapping.get('label_column', '')
            if label_column:
                pred_to_column['http://www.w3.org/2000/01/rdf-schema#label'] = label_column
            for pred_uri, pred_info in mapping.get('predicates', {}).items():
                if pred_info.get('type') == 'column':
                    pred_to_column[pred_uri] = pred_info['column']
                    pred_to_column[get_local_name(pred_uri)] = pred_info['column']
            
            where_conditions = []
            for vf in value_filters:
                pred_uri = vf['predicate_uri']
                column = pred_to_column.get(pred_uri) or pred_to_column.get(get_local_name(pred_uri))
                if column:
                    operator = vf['operator']
                    value = vf['value'].replace("'", "''")
                    if operator == 'contains':
                        where_conditions.append(f"LOWER(e.{column}) LIKE '%{value}%'")
                    elif operator == 'equals':
                        where_conditions.append(f"{SparqlTranslator._cast_str('e.' + column, dialect)} = '{value}'")
                    elif operator == 'starts':
                        where_conditions.append(f"LOWER(e.{column}) LIKE '{value}%'")
                    elif operator == 'ends':
                        where_conditions.append(f"LOWER(e.{column}) LIKE '%{value}'")
                    elif operator in ('gt', 'lt', 'gte', 'lte'):
                        op_map = {'gt': '>', 'lt': '<', 'gte': '>=', 'lte': '<='}
                        where_conditions.append(f"e.{column} {op_map[operator]} {value}")
            
            if where_conditions:
                where_clause = f"WHERE {' AND '.join(where_conditions)}"
                seed_unions.append(f"SELECT {subject_expr} AS entity_uri FROM {from_clause} AS e {where_clause}")
                logger.debug("Seed query for %s: %s", class_uri, where_clause)
        
        # Step 2: Build entity attribute queries
        entity_queries = []
        for class_uri, mapping in (mappings or {}).items():
            if filter_class_uris is not None:
                if class_uri not in filter_class_uris and get_local_name(class_uri) not in filter_local_names:
                    continue
            
            table = mapping.get('table')
            sql_query = (mapping.get('sql_query') or '').strip()
            source_key = sql_query or table
            if not source_key:
                continue
            from_clause = SparqlTranslator._source_from_alias(source_key, source_alias)
            
            id_column = mapping.get('id_column', 'id')
            label_column = mapping.get('label_column', '')
            uri_template = mapping.get('uri_template', '')
            subject_expr = get_subject_expr(uri_template, id_column, "e")
            
            predicates = []
            predicates.append(('http://www.w3.org/1999/02/22-rdf-syntax-ns#type', f"'{class_uri}'"))
            if label_column:
                predicates.append(('http://www.w3.org/2000/01/rdf-schema#label', SparqlTranslator._coalesce_cast_str(f"e.{label_column}", dialect)))
            for pred_uri, pred_info in mapping.get('predicates', {}).items():
                if pred_info.get('type') == 'column':
                    column = pred_info['column']
                    if column != label_column:
                        predicates.append((pred_uri, SparqlTranslator._coalesce_cast_str(f"e.{column}", dialect)))
            
            if predicates:
                if class_uri in filtered_entity_types:
                    where_cl = f"\n                WHERE {subject_expr} IN (SELECT entity_uri FROM seed_entities)"
                else:
                    where_cl = ""
                entity_queries.append(f"""
                    {SparqlTranslator._unpivot_select(subject_expr, predicates, f'{from_clause} AS e', where_cl, dialect)}""")
        
        # Step 3: Build relationship queries (only where at least one end is a filtered entity)
        rel_queries = []
        for rel in (relationship_mappings or []):
            predicate = rel.get('predicate', '')
            if relationship_filter:
                if not any(predicate == f or get_local_name(predicate) == get_local_name(f) for f in relationship_filter):
                    continue
            
            rel_sql = (rel.get('sql_query') or '').strip()
            subject_template = rel.get('subject_template', '')
            object_template = rel.get('object_template', '')
            subject_column = rel.get('subject_column')
            object_column = rel.get('object_column')
            
            if not rel_sql or not subject_column or not object_column:
                continue
            
            rel_from = SparqlTranslator._source_from_alias(rel_sql, source_alias, "r")
            subj_expr = get_subject_expr(subject_template, subject_column, "r")
            obj_expr = get_subject_expr(object_template, object_column, "r")
            
            rel_queries.append(f"""
                SELECT {subj_expr} AS subject, '{predicate}' AS predicate, {obj_expr} AS object
                FROM {rel_from}
                WHERE {subj_expr} IN (SELECT entity_uri FROM seed_entities)
                   OR {obj_expr} IN (SELECT entity_uri FROM seed_entities)""")
        
        # Build final SQL merging source CTEs with seed_entities
        limit_clause = f"LIMIT {limit}" if limit else ""

        cte_parts = []
        for alias, sql_text in cte_defs:
            if "SELECT" in sql_text.upper():
                cte_parts.append(f"  {alias} AS (\n    {sql_text}\n  )")
            else:
                cte_parts.append(f"  {alias} AS (\n    SELECT * FROM {sql_text}\n  )")
        seed_cte = f"  seed_entities AS (\n    {' UNION ALL '.join(seed_unions) if seed_unions else 'SELECT NULL AS entity_uri WHERE 1=0'}\n  )"
        cte_parts.append(seed_cte)

        sql = f"""WITH\n{','.join(cte_parts)}
    SELECT subject, predicate, object FROM (
        {' UNION ALL '.join(entity_queries + rel_queries)}
    ) final_results
    {limit_clause}"""
        
        logger.debug("Generated CTE-based SQL with %d source CTEs (%s chars)", len(cte_defs), len(sql))
        
        return {
            'success': True,
            'sql': sql,
            'variables': ['subject', 'predicate', 'object'],
            'message': f'Query with value filters and relationships'
        }


    @staticmethod
    def _build_generic_triples_query(mappings, select_vars, is_distinct, limit, relationship_mappings=None, filter_class_uris=None, predicate_filter=None, value_filters=None, relationship_filter=None, dialect=DIALECT_SPARK):
        """Build a query that returns all triples from mapped tables.
        
        Args:
            mappings: Entity mappings from R2RML
            select_vars: Variables to select
            is_distinct: Whether to use DISTINCT
            limit: Maximum results
            relationship_mappings: Relationship mappings from R2RML
            filter_class_uris: Optional list of class URIs to filter by (only include these entity types)
            predicate_filter: Optional list of predicate URIs to filter by (only include these predicates)
            value_filters: Optional list of value filter dicts (predicate_uri, operator, value)
            relationship_filter: Optional list of relationship URIs to include (even when filter_class_uris is set)
        """
        if not mappings and not relationship_mappings:
            return {
                'success': False,
                'message': 'No R2RML mappings found. Please load an R2RML mapping first.'
            }
        
        # Note: When relationships are included with value filters, we DON'T use CTE-based filtering
        # because the SPARQL filter applies to specific entity types only (e.g., filter on Person doesn't apply to Manager)
        # The current SPARQL parsing doesn't track which block a filter belongs to, so we return all entities
        # and let the user see the complete graph with relationships
        
        table_queries = []
        
        def get_local_name(uri):
            return _extract_local(uri).lower()

        # Helper to extract class name from URI template (e.g., "http://example.org/Person/{id}" -> "person")
        def get_class_from_template(template):
            if not template:
                return None
            clean = re.sub(r'\{[^}]+\}$', '', template).rstrip('/')
            # Get the last path segment (class name)
            if '/' in clean:
                return clean.split('/')[-1].lower()
            return None
        
        # Build filter set with both full URIs and local names for classes
        filter_local_names = None
        if filter_class_uris:
            filter_local_names = set(get_local_name(uri) for uri in filter_class_uris)
            logger.debug("Filtering by class URIs: %s", filter_class_uris)
            logger.debug("Filter local names: %s", filter_local_names)
        
        # When relationships are included, we need to also include entity classes used in those relationships
        # This is crucial for recursive relationships (e.g., Person -> CollaborateWith -> Person)
        # where the target entity might not be in the original filter but needs its attributes
        relationship_class_names = set()
        if relationship_filter and relationship_mappings:
            for rel in relationship_mappings:
                predicate = rel.get('predicate', '')
                # Check if this relationship is in the filter
                matches = False
                for filter_uri in relationship_filter:
                    if predicate == filter_uri or get_local_name(predicate) == get_local_name(filter_uri):
                        matches = True
                        break
                if matches:
                    # Extract class names from subject and object templates
                    subj_class = get_class_from_template(rel.get('subject_template', ''))
                    obj_class = get_class_from_template(rel.get('object_template', ''))
                    if subj_class:
                        relationship_class_names.add(subj_class)
                    if obj_class:
                        relationship_class_names.add(obj_class)
            if relationship_class_names:
                logger.debug("Classes from included relationships: %s", relationship_class_names)
        
        # Build predicate filter set with both full URIs and local names
        # IMPORTANT: When value_filters are present, ignore predicate_filter
        # because the user wants to filter BY an attribute, not restrict OUTPUT to that attribute
        predicate_filter_set = None
        predicate_filter_local = None
        if predicate_filter and not value_filters:
            predicate_filter_set = set(p.lower() for p in predicate_filter)
            predicate_filter_local = set(get_local_name(p) for p in predicate_filter)
            logger.debug("Filtering by predicate URIs: %s", predicate_filter)
            logger.debug("Predicate filter local names: %s", predicate_filter_local)
        elif predicate_filter and value_filters:
            logger.debug("IGNORING predicate filter because value_filters are present (user wants ALL attributes of filtered entities)")
        
        def predicate_matches_filter(pred_uri):
            """Check if a predicate URI matches the filter."""
            if predicate_filter_set is None:
                return True  # No filter, include all
            # Check exact match (case-insensitive)
            if pred_uri.lower() in predicate_filter_set:
                return True
            # Check local name match
            if predicate_filter_local and get_local_name(pred_uri) in predicate_filter_local:
                return True
            return False
        
        # Collect CTEs to deduplicate source table scans
        cte_defs, source_alias = SparqlTranslator._collect_source_ctes(mappings, relationship_mappings)

        # Entity mappings (rdf:type, rdfs:label, and all attributes)
        for class_uri, mapping in (mappings or {}).items():
            # If filter_class_uris is specified, only include matching classes
            if filter_class_uris is not None:
                class_local = get_local_name(class_uri)
                # Check exact URI match
                if class_uri in filter_class_uris:
                    logger.debug("Including class (exact match): %s", class_uri)
                # Check local name match (more flexible)
                elif filter_local_names and class_local in filter_local_names:
                    logger.debug("Including class (local name match): %s", class_uri)
                # IMPORTANT: Also include classes used in relationships (for recursive relationships)
                elif relationship_class_names and class_local in relationship_class_names:
                    logger.debug("Including class (from relationship templates): %s", class_uri)
                else:
                    logger.debug("SKIPPING class (no match): %s", class_uri)
                    continue
            table = mapping.get('table')
            sql_query = (mapping.get('sql_query') or '').strip()
            source_key = sql_query or table
            if not source_key:
                continue
            from_clause = SparqlTranslator._source_from_alias(source_key, source_alias)
            
            uri_template = mapping.get('uri_template', '')
            id_column = mapping.get('id_column', 'id')
            label_column = mapping.get('label_column', '')
            
            logger.debug("Entity URI template for %s: %s", class_uri, uri_template)
            
            subject_expr = SparqlTranslator._subject_expr_from_template(uri_template, id_column, dialect=dialect)
            
            predicates = []
            
            rdf_type_uri = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
            if predicate_matches_filter(rdf_type_uri):
                predicates.append((rdf_type_uri, f"'{class_uri}'"))
            
            rdfs_label_uri = 'http://www.w3.org/2000/01/rdf-schema#label'
            if label_column and predicate_matches_filter(rdfs_label_uri):
                predicates.append((rdfs_label_uri, SparqlTranslator._coalesce_cast_str(label_column, dialect)))
            
            for pred_uri, pred_info in mapping.get('predicates', {}).items():
                if pred_info.get('type') == 'column':
                    column = pred_info['column']
                    if column != label_column and predicate_matches_filter(pred_uri):
                        predicates.append((pred_uri, SparqlTranslator._coalesce_cast_str(column, dialect)))
            
            # Build WHERE conditions from value filters
            # IMPORTANT: When relationships are included, we want ALL entities to have attributes
            # (so related entities are also enriched), not just filtered ones.
            # The filtering is done via relationships connecting to filtered entities.
            where_conditions = []
            should_apply_value_filter = value_filters and not relationship_filter
            
            if value_filters and should_apply_value_filter:
                # Build mapping from predicate URI to column for this entity
                pred_to_column = {}
                if label_column:
                    pred_to_column['http://www.w3.org/2000/01/rdf-schema#label'] = label_column
                for pred_uri, pred_info in mapping.get('predicates', {}).items():
                    if pred_info.get('type') == 'column':
                        pred_to_column[pred_uri] = pred_info['column']
                        # Also map by local name
                        local_name = get_local_name(pred_uri)
                        pred_to_column[local_name] = pred_info['column']
                
                # Generate WHERE conditions for each value filter
                for vf in value_filters:
                    pred_uri = vf['predicate_uri']
                    column = pred_to_column.get(pred_uri) or pred_to_column.get(get_local_name(pred_uri))
                    
                    if column:
                        operator = vf['operator']
                        value = vf['value'].replace("'", "''")  # Escape single quotes
                        
                        if operator == 'contains':
                            where_conditions.append(f"LOWER({column}) LIKE '%{value}%'")
                        elif operator == 'equals':
                            where_conditions.append(f"{SparqlTranslator._cast_str(column, dialect)} = '{value}'")
                        elif operator == 'starts':
                            where_conditions.append(f"LOWER({column}) LIKE '{value}%'")
                        elif operator == 'ends':
                            where_conditions.append(f"LOWER({column}) LIKE '%{value}'")
                        elif operator == 'gt':
                            where_conditions.append(f"{column} > {value}")
                        elif operator == 'lt':
                            where_conditions.append(f"{column} < {value}")
                        elif operator == 'gte':
                            where_conditions.append(f"{column} >= {value}")
                        elif operator == 'lte':
                            where_conditions.append(f"{column} <= {value}")
                        
                        logger.debug("Value filter condition: %s %s '%s'", column, operator, value)
            elif value_filters and relationship_filter:
                logger.debug("Skipping value filter for entity %s (relationships included - will get ALL entities for enrichment)", class_uri)
            
            where_clause = ""
            if where_conditions:
                where_clause = f"\n                WHERE {' AND '.join(where_conditions)}"
            
            if len(predicates) > 0:
                table_queries.append(f"""
                    {SparqlTranslator._unpivot_select(subject_expr, predicates, from_clause, where_clause, dialect)}
                """)
        
        # Relationship mappings - include relationships that connect the filtered entity types
        # When filter_class_uris is set, include ONLY relationships where BOTH subject and object match filtered types
        if relationship_filter:
            logger.debug("Including relationships that match explicit filter: %s", relationship_filter)
        elif filter_class_uris:
            logger.debug("Including ONLY relationships connecting filtered entity types: %s", filter_class_uris)
        else:
            logger.debug("Including all relationships (no filter)")
        
        # Helper to check if a relationship matches the filter
        def relationship_matches_filter(rel_uri, subject_template, object_template):
            # If explicit relationship filter is set, use it
            if relationship_filter is not None:
                for filter_uri in relationship_filter:
                    if rel_uri == filter_uri:
                        return True
                    if get_local_name(rel_uri) == get_local_name(filter_uri):
                        return True
                return False
            
            # When filtering by entity types, only include relationships where
            # BOTH subject and object entity types are in the filtered set
            if filter_class_uris is not None:
                # Extract entity type from templates
                subj_class = get_class_from_template(subject_template)
                obj_class = get_class_from_template(object_template)
                
                # Check if both subject and object classes are in the filter
                subj_matches = subj_class and subj_class in filter_local_names
                obj_matches = obj_class and obj_class in filter_local_names
                
                if not subj_matches or not obj_matches:
                    logger.debug("  Relationship connects %s -> %s, but only %s are filtered", subj_class, obj_class, filter_local_names)
                    return False
                return True
            
            # No filter - include all relationships
            return True
        
        for rel in (relationship_mappings or []):
            predicate = rel.get('predicate', '')
            subject_template = rel.get('subject_template', '')
            object_template = rel.get('object_template', '')
            
            # Skip relationships that don't match the filter
            if not relationship_matches_filter(predicate, subject_template, object_template):
                logger.debug("Skipping relationship (entities not in filter): %s", predicate)
                continue
            
            logger.debug("Including relationship: %s", predicate)
            rel_sql_query = (rel.get('sql_query') or '').strip()
            predicate = rel.get('predicate')
            subject_template = rel.get('subject_template', '')
            object_template = rel.get('object_template', '')
            subject_column = rel.get('subject_column')
            object_column = rel.get('object_column')
            
            if not rel_sql_query or not predicate or not subject_column or not object_column:
                continue
            
            rel_from = SparqlTranslator._source_from_alias(rel_sql_query, source_alias, "rel_subquery")

            subject_expr = SparqlTranslator._subject_expr_from_template(subject_template, subject_column, dialect=dialect)
            object_expr = SparqlTranslator._subject_expr_from_template(object_template, object_column, dialect=dialect)
            
            logger.debug("Relationship URI templates for %s:", predicate)
            logger.debug("  subject_template: %s", subject_template)
            logger.debug("  object_template: %s", object_template)
            logger.debug("  subject_expr: %s", subject_expr)
            logger.debug("  object_expr: %s", object_expr)
            
            rel_query = f"""
                SELECT 
                    {subject_expr} AS subject,
                    '{predicate}' AS predicate,
                    {object_expr} AS object
                FROM {rel_from}
            """
            table_queries.append(rel_query)
            logger.debug("Added relationship query for %s", predicate)
        
        logger.debug("Total table_queries: %s", len(table_queries))
        
        if not table_queries:
            return {
                'success': False,
                'message': 'No tables found in R2RML mapping.'
            }
        
        distinct_str = "DISTINCT " if is_distinct else ""
        limit_clause = f"\n    LIMIT {limit}" if limit else ""
        with_clause = SparqlTranslator._build_with_clause(cte_defs)
        sql = f"""{with_clause}SELECT {distinct_str}subject, predicate, object FROM (
            {' UNION ALL '.join(table_queries)}
        ) AS triples
        WHERE object IS NOT NULL AND TRIM(object) != ''{limit_clause}"""
        
        logger.debug("Generated SQL with %d CTEs (length: %s chars):", len(cte_defs), len(sql))
        for i, tq in enumerate(table_queries):
            logger.debug("Subquery %s: %s...", i, tq[:300])
        
        return {
            'success': True,
            'sql': sql,
            'variables': ['subject', 'predicate', 'object']
        }


    @staticmethod
    def _get_table_source(mapping, alias=None):
        """Get table source from mapping - either table name or sql_query as subquery."""
        table = mapping.get('table')
        sql_query = mapping.get('sql_query', '').strip()
        
        if sql_query:
            # Use sql_query as a subquery with an alias
            if alias:
                return f"({sql_query}) AS {alias}"
            else:
                # Generate an alias from the class URI if available
                return f"({sql_query}) AS entity_data"
        elif table:
            return table
        else:
            return None


    @staticmethod
    def _build_spark_sql(patterns, optional_patterns, mappings, select_vars, is_distinct, limit, relationship_mappings=None, bind_values=None, predicate_filter=None, value_filters=None, relationship_filter=None, dialect=DIALECT_SPARK):
        """Build SQL from parsed patterns using R2RML mappings."""
        
        logger.debug("_build_spark_sql called with %s patterns, select_vars=%s", len(patterns), select_vars)
        if predicate_filter:
            logger.debug("Predicate filter: %s", predicate_filter)
        if value_filters:
            logger.debug("Value filters: %s", len(value_filters))
        if relationship_filter:
            logger.debug("Relationship filter: %s", relationship_filter)
        
        # Check for pure generic triple pattern: SELECT ?s ?p ?o WHERE { ?s ?p ?o }
        if SparqlTranslator._is_generic_triple_pattern(patterns, select_vars):
            logger.debug("Using generic triple pattern")
            # If we have BOTH value filters AND relationships, use the CTE-based query
            # This ensures filtered entities AND their related entities are returned (not ALL entities)
            if value_filters and relationship_filter:
                logger.debug("Using CTE-based filtered query with relationships")
                return SparqlTranslator._build_filtered_with_relationships_query(
                    mappings, select_vars, is_distinct, limit, relationship_mappings,
                    None, predicate_filter, value_filters, relationship_filter, dialect=dialect
                )
            return SparqlTranslator._build_generic_triples_query(mappings, select_vars, is_distinct, limit, relationship_mappings, None, predicate_filter, value_filters, relationship_filter, dialect=dialect)
        
        # Check for type-constrained triple pattern: SELECT ?s ?p ?o WHERE { ?s rdf:type <Entity> . ?s ?p ?o }
        logger.debug("Checking for type-constrained pattern...")
        filter_class_uris = SparqlTranslator._is_type_constrained_triple_pattern(patterns)
        if filter_class_uris:
            logger.debug("Detected type-constrained triple pattern for classes: %s", filter_class_uris)
            # If we have BOTH value filters AND relationships, use the CTE-based query
            if value_filters and relationship_filter:
                logger.debug("Using CTE-based filtered query with relationships (type-constrained)")
                return SparqlTranslator._build_filtered_with_relationships_query(
                    mappings, select_vars, is_distinct, limit, relationship_mappings,
                    filter_class_uris, predicate_filter, value_filters, relationship_filter, dialect=dialect
                )
            return SparqlTranslator._build_generic_triples_query(mappings, select_vars, is_distinct, limit, relationship_mappings, filter_class_uris, predicate_filter, value_filters, relationship_filter, dialect=dialect)
        
        logger.debug("Falling through to standard SQL builder")
        
        var_to_class = {}
        var_to_table = {}
        var_to_table_alias = {}
        var_to_mapping = {}
        
        # Initialize bind_values if None
        if bind_values is None:
            bind_values = {}
        
        rdf_type = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
        
        for pattern in patterns:
            if pattern['predicate'] == rdf_type and not pattern['object_is_var']:
                class_uri = pattern['object']
                var_name = pattern['subject_var']
                
                if class_uri in mappings:
                    mapping = mappings[class_uri]
                    var_to_class[var_name] = class_uri
                    var_to_mapping[var_name] = mapping
                    var_to_table[var_name] = SparqlTranslator._get_table_source(mapping, f"t_{var_name}")
                    var_to_table_alias[var_name] = f"t_{var_name}" if mapping.get('sql_query') else mapping.get('table')
        
        if not var_to_class:
            for pattern in patterns:
                if not pattern['predicate_is_var'] and pattern['subject_is_var']:
                    pred_uri = pattern['predicate']
                    var_name = pattern['subject_var']
                    
                    for class_uri, mapping in mappings.items():
                        if pred_uri in mapping.get('predicates', {}):
                            if var_name not in var_to_class:
                                var_to_class[var_name] = class_uri
                                var_to_mapping[var_name] = mapping
                                var_to_table[var_name] = SparqlTranslator._get_table_source(mapping, f"t_{var_name}")
                                var_to_table_alias[var_name] = f"t_{var_name}" if mapping.get('sql_query') else mapping.get('table')
                            break
        
        if not var_to_table or all(v is None for v in var_to_table.values()):
            return {
                'success': False,
                'message': 'Could not determine which tables to query. Ensure your mapping has table or sql_query defined.'
            }
        
        # Build SELECT columns
        select_columns = []
        all_vars = set()
        
        for pattern in patterns + optional_patterns:
            if pattern['subject_is_var']:
                all_vars.add(pattern['subject_var'])
            if pattern['predicate_is_var']:
                all_vars.add(pattern['predicate_var'])
            if pattern['object_is_var']:
                all_vars.add(pattern['object_var'])
        
        if select_vars is None:
            select_vars = list(all_vars)
        
        var_to_column = {}
        from_tables = []  # Changed to list to preserve order and handle subqueries
        where_conditions = []
        
        # Add BIND values as literal columns
        for var_name, value in bind_values.items():
            var_to_column[var_name] = f"'{value}'"
            logger.debug("Added BIND value to var_to_column: %s = '%s'", var_name, value)
        
        # Collect relationship info to determine which entities should be joined vs primary
        relationship_patterns = []
        for pattern in patterns + optional_patterns:
            if (pattern['subject_is_var'] and pattern['object_is_var'] and
                pattern['subject_var'] in var_to_mapping and pattern['object_var'] in var_to_mapping and
                not pattern['predicate_is_var'] and pattern['predicate'] != rdf_type):
                relationship_patterns.append(pattern)
        
        # Determine which entities are "joined" (object of a relationship)
        joined_entity_vars = set()
        for rp in relationship_patterns:
            # The object entity of a relationship should be joined, not in main FROM
            joined_entity_vars.add(rp['object_var'])
        
        # Add entity tables to FROM - but only primary entities (not those joined via relationships)
        for var_name, mapping in var_to_mapping.items():
            table_source = var_to_table.get(var_name)
            table_alias = var_to_table_alias.get(var_name)
            
            # Only add to FROM if this is NOT a joined entity (or if no relationships exist)
            if table_source and table_source not in from_tables:
                if var_name not in joined_entity_vars or len(relationship_patterns) == 0:
                    from_tables.append(table_source)
                else:
                    logger.debug("Entity %s will be joined via relationship, not adding to main FROM", var_name)
                
            if table_alias:
                if mapping.get('uri_template'):
                    template = mapping['uri_template']
                    id_col = mapping.get('id_column', 'id')
                    var_to_column[var_name] = f"CONCAT('{template.split('{')[0]}', {table_alias}.{id_col})"
                elif mapping.get('id_column'):
                    var_to_column[var_name] = f"{table_alias}.{mapping['id_column']}"
        
        # Process property patterns
        for pattern in patterns:
            if pattern['predicate'] == rdf_type:
                continue
            
            # Check if this is a relationship join pattern (both subject and object are entity variables)
            if (pattern['subject_is_var'] and pattern['object_is_var'] and
                pattern['subject_var'] in var_to_mapping and pattern['object_var'] in var_to_mapping and
                not pattern['predicate_is_var']):
                
                pred_uri = pattern['predicate']
                subject_alias = var_to_table_alias.get(pattern['subject_var'])
                object_alias = var_to_table_alias.get(pattern['object_var'])
                subject_mapping = var_to_mapping[pattern['subject_var']]
                object_mapping = var_to_mapping[pattern['object_var']]
                
                logger.debug("Detected entity-to-entity relationship pattern: %s --%s--> %s", pattern['subject_var'], pred_uri, pattern['object_var'])
                
                # Look for this relationship in relationship_mappings
                if relationship_mappings:
                    for rel in relationship_mappings:
                        rel_pred = rel.get('predicate', '')
                        # Match by full URI or local name
                        pred_local = pred_uri.split('#')[-1].split('/')[-1] if pred_uri else ''
                        rel_local = rel_pred.split('#')[-1].split('/')[-1] if rel_pred else ''
                        
                        if rel_pred == pred_uri or (pred_local and pred_local == rel_local):
                            rel_sql = rel.get('sql_query', '').strip()
                            source_col = rel.get('subject_column')
                            target_col = rel.get('object_column')
                            
                            logger.debug("Found relationship mapping: sql=%s...", rel_sql[:50] if rel_sql else 'N/A')
                            
                            if rel_sql and source_col and target_col:
                                rel_alias = f"rel_{pattern['subject_var']}_{pattern['object_var']}"
                                subject_id = subject_mapping.get('id_column', 'id')
                                object_id = object_mapping.get('id_column', 'id')
                                
                                # Check if object entity is "joined" (not in main FROM)
                                if pattern['object_var'] in joined_entity_vars:
                                    # First: Join relationship table to subject
                                    rel_join = f"INNER JOIN ({rel_sql}) AS {rel_alias} ON {subject_alias}.{subject_id} = {rel_alias}.{source_col}"
                                    from_tables.append(rel_join)
                                    
                                    # Second: Join target entity table via relationship
                                    object_sql = object_mapping.get('sql_query', '').strip()
                                    if object_sql:
                                        entity_join = f"INNER JOIN ({object_sql}) AS {object_alias} ON {rel_alias}.{target_col} = {object_alias}.{object_id}"
                                    else:
                                        entity_join = f"INNER JOIN {object_mapping.get('table')} AS {object_alias} ON {rel_alias}.{target_col} = {object_alias}.{object_id}"
                                    from_tables.append(entity_join)
                                    logger.debug("Added relationship + entity join: %s -> %s", rel_alias, object_alias)
                                else:
                                    # Both entities in FROM, just add relationship constraint
                                    join_clause = f"INNER JOIN ({rel_sql}) AS {rel_alias} ON {subject_alias}.{subject_id} = {rel_alias}.{source_col} AND {object_alias}.{object_id} = {rel_alias}.{target_col}"
                                    from_tables.append(join_clause)
                                    logger.debug("Added relationship join: %s...", join_clause[:100])
                            break
                continue  # Skip normal property processing for this pattern
            
            if pattern['subject_is_var'] and pattern['subject_var'] in var_to_mapping:
                mapping = var_to_mapping[pattern['subject_var']]
                table_alias = var_to_table_alias.get(pattern['subject_var'])
                
                if not pattern['predicate_is_var'] and table_alias:
                    pred_uri = pattern['predicate']
                    pred_info = mapping.get('predicates', {}).get(pred_uri)
                    
                    if pred_info:
                        if pred_info.get('type') == 'column':
                            column = pred_info['column']
                            
                            if pattern['object_is_var']:
                                var_to_column[pattern['object_var']] = f"{table_alias}.{column}"
                            else:
                                obj_value = pattern['object'].strip('"')
                                where_conditions.append(f"{table_alias}.{column} = '{_escape_sql(obj_value)}'")
                        
                        elif pred_info.get('type') == 'reference':
                            child_col = pred_info.get('child_column')
                            parent_col = pred_info.get('parent_column')
                            
                            for class_uri, m in mappings.items():
                                if pattern['object_is_var']:
                                    obj_var = pattern['object_var']
                                    if obj_var in var_to_mapping:
                                        parent_table_source = var_to_table.get(obj_var)
                                        parent_table_alias = var_to_table_alias.get(obj_var)
                                        if parent_table_source and parent_table_alias and child_col and parent_col:
                                            if parent_table_source not in from_tables:
                                                from_tables.append(parent_table_source)
                                            where_conditions.append(f"{table_alias}.{child_col} = {parent_table_alias}.{parent_col}")
                    
                    # Check if this is a relationship predicate (object is NOT an entity variable)
                    elif relationship_mappings and pattern['object_is_var'] and pattern['object_var'] not in var_to_mapping:
                        logger.debug("Main pattern: Looking for relationship predicate: %s", pred_uri)
                        
                        matched_rel = None
                        for rel in relationship_mappings:
                            rel_pred = rel.get('predicate', '')
                            if rel_pred == pred_uri:
                                matched_rel = rel
                                break
                            pred_local = pred_uri.split('#')[-1].split('/')[-1] if pred_uri else ''
                            rel_local = rel_pred.split('#')[-1].split('/')[-1] if rel_pred else ''
                            if pred_local and pred_local == rel_local:
                                matched_rel = rel
                                break
                        
                        if matched_rel:
                            rel_sql = matched_rel.get('sql_query', '').strip()
                            subject_col = matched_rel.get('subject_column')
                            object_col = matched_rel.get('object_column')
                            
                            logger.debug("Found relationship for main pattern: sql=%s...", rel_sql[:50] if rel_sql else 'N/A')
                            
                            if rel_sql and subject_col and object_col:
                                obj_var = pattern['object_var']
                                rel_alias = f"rel_{obj_var.lstrip('?')}"
                                
                                # INNER JOIN for mandatory relationship pattern
                                join_clause = f"INNER JOIN ({rel_sql}) AS {rel_alias} ON {table_alias}.{mapping.get('id_column', 'id')} = {rel_alias}.{subject_col}"
                                from_tables.append(join_clause)
                                
                                target_mapping = var_to_mapping.get(obj_var)
                                if target_mapping and target_mapping.get('uri_template'):
                                    template = target_mapping['uri_template']
                                    uri_base = template.split('{')[0]
                                    var_to_column[obj_var] = f"CONCAT('{uri_base}', {SparqlTranslator._cast_str(f'{rel_alias}.{object_col}', dialect)})"
                                    logger.debug("Mapped %s to URI: CONCAT('%s', %s.%s)", obj_var, uri_base, rel_alias, object_col)
                                else:
                                    var_to_column[obj_var] = f"{rel_alias}.{object_col}"
                                    logger.debug("Mapped %s to raw column: %s.%s", obj_var, rel_alias, object_col)
                                
                                # Join the target entity table to get the label
                                # For self-referencing relationships, the target is the same entity type
                                target_entity_mapping = None
                                
                                # First check: if this is a self-referencing relationship, use the same mapping
                                if mapping == var_to_mapping.get(pattern['subject_var']):
                                    # Could be self-referencing - check if object column references same entity
                                    # Strip trailing numbers and underscores from object_col
                                    base_obj_col = re.sub(r'[_\d]+$', '', object_col)
                                    id_col = mapping.get('id_column', '')
                                    base_id_col = re.sub(r'[_\d]+$', '', id_col)
                                    
                                    if base_obj_col and base_id_col and base_obj_col == base_id_col:
                                        target_entity_mapping = mapping
                                        logger.debug("Self-referencing relationship detected, using same entity mapping")
                                
                                # Second check: match by id column similarity
                                if not target_entity_mapping:
                                    for class_uri, class_mapping in mappings.items():
                                        id_col = class_mapping.get('id_column', '')
                                        if id_col:
                                            # Strip trailing numbers for comparison
                                            base_id = re.sub(r'[_\d]+$', '', id_col)
                                            base_obj = re.sub(r'[_\d]+$', '', object_col)
                                            if base_id and base_obj and base_id == base_obj:
                                                target_entity_mapping = class_mapping
                                                break
                                
                                if target_entity_mapping:
                                    target_sql = target_entity_mapping.get('sql_query', '').strip()
                                    target_label_col = target_entity_mapping.get('label_column')
                                    target_id_col = target_entity_mapping.get('id_column', 'id')
                                    
                                    if target_sql and target_label_col:
                                        target_alias = f"target_{obj_var.lstrip('?')}"
                                        target_join = f"LEFT JOIN ({target_sql}) AS {target_alias} ON {rel_alias}.{object_col} = {target_alias}.{target_id_col}"
                                        from_tables.append(target_join)
                                        
                                        # Map the label variable
                                        label_var = f"{obj_var}_label"
                                        var_to_column[label_var] = f"{target_alias}.{target_label_col}"
                                        logger.debug("Added target entity join for main pattern: %s, label mapped to %s", target_alias, label_var)
        
        # Track optional relationship conditions for OR filter
        optional_rel_conditions = []
        
        # Process OPTIONAL patterns
        for pattern in optional_patterns:
            # Check if this is an OPTIONAL entity-to-entity relationship
            if (pattern['subject_is_var'] and pattern['object_is_var'] and
                pattern['subject_var'] in var_to_mapping and pattern['object_var'] in var_to_mapping and
                not pattern['predicate_is_var']):
                
                pred_uri = pattern['predicate']
                subject_alias = var_to_table_alias.get(pattern['subject_var'])
                object_alias = var_to_table_alias.get(pattern['object_var'])
                subject_mapping = var_to_mapping[pattern['subject_var']]
                object_mapping = var_to_mapping[pattern['object_var']]
                
                logger.debug("Detected OPTIONAL entity-to-entity relationship: %s --%s--> %s", pattern['subject_var'], pred_uri, pattern['object_var'])
                
                # Look for this relationship in relationship_mappings
                if relationship_mappings:
                    for rel in relationship_mappings:
                        rel_pred = rel.get('predicate', '')
                        pred_local = pred_uri.split('#')[-1].split('/')[-1] if pred_uri else ''
                        rel_local = rel_pred.split('#')[-1].split('/')[-1] if rel_pred else ''
                        
                        if rel_pred == pred_uri or (pred_local and pred_local == rel_local):
                            rel_sql = rel.get('sql_query', '').strip()
                            source_col = rel.get('subject_column')
                            target_col = rel.get('object_column')
                            
                            if rel_sql and source_col and target_col:
                                rel_alias = f"optrel_{pattern['subject_var']}_{len(optional_rel_conditions)}"
                                subject_id = subject_mapping.get('id_column', 'id')
                                object_id = object_mapping.get('id_column', 'id')
                                
                                # Check if object entity is "joined" (not in main FROM)
                                if pattern['object_var'] in joined_entity_vars:
                                    # LEFT JOIN relationship table, then LEFT JOIN entity table
                                    rel_join = f"LEFT JOIN ({rel_sql}) AS {rel_alias} ON {subject_alias}.{subject_id} = {rel_alias}.{source_col}"
                                    from_tables.append(rel_join)
                                    
                                    # LEFT JOIN target entity table
                                    object_sql = object_mapping.get('sql_query', '').strip()
                                    if object_sql:
                                        entity_join = f"LEFT JOIN ({object_sql}) AS {object_alias} ON {rel_alias}.{target_col} = {object_alias}.{object_id}"
                                    else:
                                        entity_join = f"LEFT JOIN {object_mapping.get('table')} AS {object_alias} ON {rel_alias}.{target_col} = {object_alias}.{object_id}"
                                    from_tables.append(entity_join)
                                    logger.debug("Added optional relationship + entity LEFT JOIN: %s -> %s", rel_alias, object_alias)
                                else:
                                    # Both in FROM - add relationship join with both conditions
                                    join_clause = f"LEFT JOIN ({rel_sql}) AS {rel_alias} ON {subject_alias}.{subject_id} = {rel_alias}.{source_col} AND {object_alias}.{object_id} = {rel_alias}.{target_col}"
                                    from_tables.append(join_clause)
                                    logger.debug("Added optional relationship LEFT JOIN: %s", rel_alias)
                                
                                # Track for OR condition
                                optional_rel_conditions.append(f"{rel_alias}.{source_col} IS NOT NULL")
                            break
                continue
            
            if pattern['subject_is_var'] and pattern['subject_var'] in var_to_mapping:
                mapping = var_to_mapping[pattern['subject_var']]
                table_alias = var_to_table_alias.get(pattern['subject_var'])
                
                if not pattern['predicate_is_var'] and table_alias:
                    pred_uri = pattern['predicate']
                    pred_info = mapping.get('predicates', {}).get(pred_uri)
                    
                    if pred_info and pred_info.get('type') == 'column':
                        column = pred_info['column']
                        if pattern['object_is_var']:
                            var_to_column[pattern['object_var']] = f"{table_alias}.{column}"
                    
                    # Check if this is a relationship predicate
                    elif relationship_mappings and pattern['object_is_var']:
                        logger.debug("Looking for relationship predicate: %s", pred_uri)
                        logger.debug("Available relationship predicates: %s", [r.get('predicate') for r in relationship_mappings])
                        
                        # Try exact match first, then try matching just the local name
                        matched_rel = None
                        for rel in relationship_mappings:
                            rel_pred = rel.get('predicate', '')
                            if rel_pred == pred_uri:
                                matched_rel = rel
                                break
                            # Try matching just the local name (after # or last /)
                            pred_local = pred_uri.split('#')[-1].split('/')[-1] if pred_uri else ''
                            rel_local = rel_pred.split('#')[-1].split('/')[-1] if rel_pred else ''
                            if pred_local and pred_local == rel_local:
                                logger.debug("Matched by local name: %s", pred_local)
                                matched_rel = rel
                                break
                        
                        if matched_rel:
                            # Found a relationship mapping
                            rel_sql = matched_rel.get('sql_query', '').strip()
                            subject_col = matched_rel.get('subject_column')
                            object_col = matched_rel.get('object_column')
                            
                            logger.debug("Found relationship: sql=%s..., subject_col=%s, object_col=%s", rel_sql[:50] if rel_sql else 'N/A', subject_col, object_col)
                            
                            if rel_sql and subject_col and object_col:
                                obj_var = pattern['object_var']
                                rel_alias = f"rel_{obj_var.lstrip('?')}"
                                join_clause = f"LEFT JOIN ({rel_sql}) AS {rel_alias} ON {table_alias}.{mapping.get('id_column', 'id')} = {rel_alias}.{subject_col}"
                                
                                target_mapping = var_to_mapping.get(obj_var)
                                if target_mapping and target_mapping.get('uri_template'):
                                    template = target_mapping['uri_template']
                                    uri_base = template.split('{')[0]
                                    var_to_column[obj_var] = f"CONCAT('{uri_base}', {SparqlTranslator._cast_str(f'{rel_alias}.{object_col}', dialect)})"
                                    logger.debug("Mapped %s to URI: CONCAT('%s', %s.%s)", obj_var, uri_base, rel_alias, object_col)
                                else:
                                    var_to_column[obj_var] = f"{rel_alias}.{object_col}"
                                    logger.debug("Mapped %s to raw column: %s.%s", obj_var, rel_alias, object_col)
                                
                                # Store the join
                                from_tables.append(join_clause)
                                
                                # Add to optional relationship conditions for OR filter
                                optional_rel_conditions.append(f"{rel_alias}.{subject_col} IS NOT NULL")
                                logger.debug("Added relationship to OR conditions: %s", rel_alias)
                                
                                # Try to find the target entity mapping to get the label
                                # Look for the target entity based on the object column name
                                target_entity_mapping = None
                                for class_uri, class_mapping in mappings.items():
                                    # Check if this mapping's id_column matches the relationship's object_column
                                    if class_mapping.get('id_column') == object_col or object_col.endswith('_id'):
                                        # This might be the target entity
                                        target_entity_mapping = class_mapping
                                        break
                                
                                if target_entity_mapping:
                                    # Join to the target entity table to get the label
                                    target_sql = target_entity_mapping.get('sql_query', '').strip()
                                    target_label_col = target_entity_mapping.get('label_column')
                                    target_id_col = target_entity_mapping.get('id_column', 'id')
                                    
                                    if target_sql and target_label_col:
                                        target_alias = f"target_{obj_var.lstrip('?')}"
                                        target_join = f"LEFT JOIN ({target_sql}) AS {target_alias} ON {rel_alias}.{object_col} = {target_alias}.{target_id_col}"
                                        from_tables.append(target_join)
                                        
                                        # Map the label variable
                                        label_var = f"{obj_var}_label"
                                        var_to_column[label_var] = f"{target_alias}.{target_label_col}"
                                        logger.debug("Added target entity join and mapped %s to %s.%s", label_var, target_alias, target_label_col)
                            else:
                                logger.debug("Relationship mapping incomplete: sql=%s, subject_col=%s, object_col=%s", bool(rel_sql), subject_col, object_col)
        
        # Build SELECT clause
        # Note: select_vars has variable names without '?', var_to_column uses '?' prefix
        logger.debug("Building SELECT clause. select_vars=%s", select_vars)
        logger.debug("var_to_column keys=%s", list(var_to_column.keys()))
        
        for var in select_vars:
            var_with_prefix = f"?{var}"
            if var_with_prefix in var_to_column:
                select_columns.append(f"{var_to_column[var_with_prefix]} AS {var}")
                logger.debug("Added %s from var_to_column (with ?)", var)
            elif var in var_to_column:
                select_columns.append(f"{var_to_column[var]} AS {var}")
                logger.debug("Added %s from var_to_column (without ?)", var)
            else:
                found = False
                for var_name, mapping in var_to_mapping.items():
                    if var == var_name:
                        if mapping.get('id_column'):
                            table_alias = var_to_table_alias.get(var_name)
                            if table_alias:
                                select_columns.append(f"{table_alias}.{mapping['id_column']} AS {var}")
                                logger.debug("Added %s from entity mapping", var)
                                found = True
                            break
                if not found:
                    logger.debug("Could not find mapping for %s", var)
        
        if not select_columns:
            return {
                'success': False,
                'message': 'Could not map any SELECT variables to table columns.'
            }
        
        distinct_str = "DISTINCT " if is_distinct else ""
        
        # Separate main tables from JOINs (LEFT JOIN, INNER JOIN, etc.)
        main_tables = []
        join_clauses = []
        for table in from_tables:
            if table.startswith('LEFT JOIN') or table.startswith('INNER JOIN') or table.startswith('JOIN'):
                join_clauses.append(table)
            else:
                main_tables.append(table)
        
        # Build FROM clause with JOINs
        from_clause = ", ".join(main_tables)
        if join_clauses:
            from_clause += " " + " ".join(join_clauses)
        
        # Add optional relationship OR condition if we have multiple optional relationships
        if optional_rel_conditions:
            or_condition = "(" + " OR ".join(optional_rel_conditions) + ")"
            where_conditions.append(or_condition)
            logger.debug("Added OR condition for optional relationships: %s", or_condition)
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        limit_clause = f"\nLIMIT {limit}" if limit else ""
        
        sql = f"""SELECT {distinct_str}{', '.join(select_columns)}
    FROM {from_clause}
    WHERE {where_clause}{limit_clause}"""
        
        return {
            'success': True,
            'sql': sql,
            'variables': select_vars
        }



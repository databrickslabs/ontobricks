"""
Tests for SQL Wizard Service - Text-to-SQL Generation

Tests cover:
- Schema context rendering
- Prompt composition with schema context
- SQL validation (SELECT-only, table whitelist, LIMIT enforcement)
- SQL extraction from LLM output
- Full generation pipeline
"""
import pytest
from unittest.mock import Mock, MagicMock, patch

from back.core.errors import InfrastructureError
from back.core.sqlwizard import SQLWizardService, SchemaContext


class TestSchemaContext:
    """Tests for SchemaContext class."""
    
    def test_schema_context_to_yaml_like(self):
        """Test that schema context is rendered in YAML-like format."""
        context = SchemaContext(
            tables=[
                {
                    'name': 'customers',
                    'full_name': 'test_catalog.test_schema.customers',
                    'columns': [
                        {'name': 'id', 'type': 'INT', 'comment': 'Primary key'},
                        {'name': 'name', 'type': 'STRING', 'comment': None},
                        {'name': 'email', 'type': 'STRING', 'comment': 'Customer email'}
                    ]
                },
                {
                    'name': 'orders',
                    'full_name': 'test_catalog.test_schema.orders',
                    'columns': [
                        {'name': 'order_id', 'type': 'INT', 'comment': None},
                        {'name': 'customer_id', 'type': 'INT', 'comment': 'FK to customers'}
                    ]
                }
            ]
        )
        
        result = context.to_yaml_like()
        
        assert 'tables:' in result
        assert 'test_catalog.test_schema.customers' in result
        assert 'test_catalog.test_schema.orders' in result
        assert 'id: INT' in result
        assert '# Primary key' in result
        assert '# Customer email' in result

    def test_schema_context_uses_name_fallback(self):
        """Test that name is used when full_name is absent."""
        context = SchemaContext(tables=[{'name': 'simple_table', 'columns': []}])
        result = context.to_yaml_like()
        assert 'simple_table' in result

    def test_schema_context_empty_tables(self):
        """Test rendering with no tables."""
        context = SchemaContext(tables=[])
        result = context.to_yaml_like()
        assert result == 'tables:'


class TestPromptComposer:
    """Tests for prompt composition."""
    
    @pytest.fixture
    def mock_client(self):
        client = Mock()
        client.host = 'https://test.databricks.com'
        client.has_valid_auth.return_value = True
        return client
    
    @pytest.fixture
    def wizard(self, mock_client):
        return SQLWizardService(mock_client)
    
    @pytest.fixture
    def sample_context(self):
        return SchemaContext(
            tables=[
                {
                    'name': 'customers',
                    'full_name': 'main.sales.customers',
                    'columns': [
                        {'name': 'id', 'type': 'INT', 'comment': None},
                        {'name': 'name', 'type': 'STRING', 'comment': None}
                    ]
                }
            ]
        )
    
    def test_compose_prompt_includes_system_instruction(self, wizard, sample_context):
        result = wizard.compose_prompt(
            user_prompt="Get all customers",
            schema_context=sample_context
        )
        
        assert 'system' in result
        assert 'user' in result
        assert 'text-to-SQL assistant' in result['system']
        assert 'SELECT statement' in result['system']
        assert 'No DDL/DML' in result['system']
    
    def test_compose_prompt_includes_schema_context(self, wizard, sample_context):
        result = wizard.compose_prompt(
            user_prompt="Get all customers",
            schema_context=sample_context
        )
        
        assert 'Available objects:' in result['user']
        assert 'main.sales.customers' in result['user']
    
    def test_compose_prompt_includes_user_task(self, wizard, sample_context):
        result = wizard.compose_prompt(
            user_prompt="Get all customers with name starting with A",
            schema_context=sample_context
        )
        
        assert 'Task: Get all customers with name starting with A' in result['user']
    
    def test_compose_prompt_includes_constraints(self, wizard, sample_context):
        result = wizard.compose_prompt(
            user_prompt="Get customers",
            schema_context=sample_context,
            limit=50
        )
        
        assert 'LIMIT 50' in result['user']
        assert 'Use only listed objects' in result['user']
        assert 'ISO date literals' in result['user']
    
    def test_compose_prompt_default_limit(self, wizard, sample_context):
        result = wizard.compose_prompt(
            user_prompt="Get customers",
            schema_context=sample_context
        )
        
        assert 'LIMIT 100' in result['user']
        assert 'LIMIT 100' in result['system']


class TestSQLValidator:
    """Tests for SQL validation."""
    
    @pytest.fixture
    def mock_client(self):
        client = Mock()
        client.host = 'https://test.databricks.com'
        client.has_valid_auth.return_value = True
        return client
    
    @pytest.fixture
    def wizard(self, mock_client):
        return SQLWizardService(mock_client)
    
    @pytest.fixture
    def sample_context(self):
        return SchemaContext(
            tables=[
                {'name': 'customers', 'full_name': 'main.sales.customers', 'columns': []},
                {'name': 'orders', 'full_name': 'main.sales.orders', 'columns': []},
                {'name': 'products', 'full_name': 'main.sales.products', 'columns': []}
            ]
        )
    
    def test_validate_select_only_passes(self, wizard, sample_context):
        sql = "SELECT id, name FROM customers"
        is_valid, message, _ = wizard.validate_sql_static(sql, sample_context)
        
        assert is_valid
        assert 'passed' in message.lower()
    
    def test_validate_rejects_create(self, wizard, sample_context):
        sql = "CREATE TABLE new_table (id INT)"
        is_valid, message, _ = wizard.validate_sql_static(sql, sample_context)
        
        assert not is_valid
        assert 'SELECT statement' in message
    
    def test_validate_rejects_drop(self, wizard, sample_context):
        sql = "DROP TABLE customers"
        is_valid, message, _ = wizard.validate_sql_static(sql, sample_context)
        
        assert not is_valid
        assert 'SELECT statement' in message
    
    def test_validate_rejects_insert(self, wizard, sample_context):
        sql = "INSERT INTO customers (id, name) VALUES (1, 'Test')"
        is_valid, message, _ = wizard.validate_sql_static(sql, sample_context)
        
        assert not is_valid
        assert 'must be a SELECT statement' in message
    
    def test_validate_rejects_update(self, wizard, sample_context):
        sql = "UPDATE customers SET name = 'Test' WHERE id = 1"
        is_valid, message, _ = wizard.validate_sql_static(sql, sample_context)
        
        assert not is_valid
        assert 'must be a SELECT statement' in message
    
    def test_validate_rejects_delete(self, wizard, sample_context):
        sql = "DELETE FROM customers WHERE id = 1"
        is_valid, message, _ = wizard.validate_sql_static(sql, sample_context)
        
        assert not is_valid
        assert 'must be a SELECT statement' in message
    
    def test_validate_table_whitelist_passes_simple_table(self, wizard, sample_context):
        sql = "SELECT * FROM customers"
        is_valid, message, _ = wizard.validate_sql_static(sql, sample_context)
        
        assert is_valid
    
    def test_validate_table_whitelist_passes_qualified_table(self, wizard, sample_context):
        sql = "SELECT * FROM main.sales.customers"
        is_valid, message, _ = wizard.validate_sql_static(sql, sample_context)
        
        assert is_valid
    
    def test_validate_table_whitelist_rejects_unknown_table(self, wizard, sample_context):
        sql = "SELECT * FROM unknown_table"
        is_valid, message, _ = wizard.validate_sql_static(sql, sample_context)
        
        assert not is_valid
        assert 'unknown_table' in message.lower()
    
    def test_validate_adds_limit_when_missing(self, wizard, sample_context):
        sql = "SELECT * FROM customers"
        is_valid, message, corrected_sql = wizard.validate_sql_static(sql, sample_context, limit=100)
        
        assert is_valid
        assert 'LIMIT 100' in corrected_sql
    
    def test_validate_preserves_existing_limit(self, wizard, sample_context):
        sql = "SELECT * FROM customers LIMIT 50"
        is_valid, message, corrected_sql = wizard.validate_sql_static(sql, sample_context, limit=100)
        
        assert is_valid
        assert 'LIMIT 50' in corrected_sql
        assert corrected_sql.count('LIMIT') == 1
    
    def test_validate_with_join(self, wizard, sample_context):
        sql = "SELECT c.id, o.order_id FROM customers c JOIN orders o ON c.id = o.customer_id"
        is_valid, message, _ = wizard.validate_sql_static(sql, sample_context)
        
        assert is_valid
    
    def test_validate_with_unknown_join_table(self, wizard, sample_context):
        sql = "SELECT c.id FROM customers c JOIN unknown_table u ON c.id = u.id"
        is_valid, message, _ = wizard.validate_sql_static(sql, sample_context)
        
        assert not is_valid
        assert 'unknown_table' in message.lower()


class TestSQLExtractor:
    """Tests for SQL extraction from LLM output."""
    
    @pytest.fixture
    def mock_client(self):
        client = Mock()
        client.host = 'https://test.databricks.com'
        client.has_valid_auth.return_value = True
        return client
    
    @pytest.fixture
    def wizard(self, mock_client):
        return SQLWizardService(mock_client)
    
    def test_extract_plain_sql(self, wizard):
        output = "SELECT id, name FROM customers"
        result = wizard.extract_sql(output)
        
        assert result == "SELECT id, name FROM customers"
    
    def test_extract_sql_from_code_fence(self, wizard):
        output = """Here's the query:

```sql
SELECT id, name FROM customers
```

This will return all customers."""
        
        result = wizard.extract_sql(output)
        assert result == "SELECT id, name FROM customers"
    
    def test_extract_sql_from_plain_code_fence(self, wizard):
        output = """
```
SELECT id, name FROM customers
```
"""
        result = wizard.extract_sql(output)
        assert result == "SELECT id, name FROM customers"
    
    def test_extract_removes_trailing_semicolon(self, wizard):
        output = "SELECT id FROM customers;"
        result = wizard.extract_sql(output)
        
        assert result == "SELECT id FROM customers"
    
    def test_extract_first_statement_only(self, wizard):
        output = "SELECT id FROM customers; SELECT * FROM orders;"
        result = wizard.extract_sql(output)
        
        assert result == "SELECT id FROM customers"
        assert 'orders' not in result
    
    def test_extract_from_prose_with_sql(self, wizard):
        output = """To get the customer data, you would use:

SELECT id, name, email
FROM customers
WHERE active = true

This returns active customers."""
        
        result = wizard.extract_sql(output)
        assert result.startswith("SELECT")
        assert "customers" in result


class TestIntegration:
    """Integration tests for the full SQL generation pipeline."""
    
    @pytest.fixture
    def mock_client(self):
        client = Mock()
        client.host = 'https://test.databricks.com'
        client.has_valid_auth.return_value = True
        client.get_tables.return_value = ['customers', 'orders']
        client.get_table_columns.return_value = [
            {'name': 'id', 'type': 'INT', 'comment': None},
            {'name': 'name', 'type': 'STRING', 'comment': None}
        ]
        client.execute_query.return_value = [{'plan': 'Scan customers'}]
        return client
    
    @pytest.fixture
    def wizard(self, mock_client):
        return SQLWizardService(mock_client)
    
    def test_get_schema_context_builds_context(self, wizard, mock_client):
        context = wizard.get_schema_context('main', 'sales')
        
        assert len(context.tables) == 2
        assert context.tables[0]['name'] == 'customers'
        assert context.tables[0]['full_name'] == 'main.sales.customers'
        mock_client.get_tables.assert_called_once_with('main', 'sales')
    
    def test_get_schema_context_uses_cache(self, wizard, mock_client):
        wizard.get_schema_context('main', 'sales')
        wizard.get_schema_context('main', 'sales')
        
        assert mock_client.get_tables.call_count == 1
    
    def test_get_schema_context_bypasses_cache(self, wizard, mock_client):
        wizard.get_schema_context('main', 'sales', use_cache=True)
        wizard.get_schema_context('main', 'sales', use_cache=False)
        
        assert mock_client.get_tables.call_count == 2
    
    def test_clear_cache(self, wizard, mock_client):
        wizard.get_schema_context('main', 'sales')
        wizard.clear_cache()
        wizard.get_schema_context('main', 'sales')
        
        assert mock_client.get_tables.call_count == 2
    
    @patch('requests.post')
    def test_generate_sql_full_pipeline(self, mock_post, wizard, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"choices":[{"message":{"content":"SELECT id, name FROM customers LIMIT 100"}}]}'
        mock_response.text = mock_response.content.decode()
        mock_response.json.return_value = {
            'choices': [{
                'message': {
                    'content': 'SELECT id, name FROM customers LIMIT 100'
                }
            }]
        }
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response
        
        result = wizard.generate_sql(
            endpoint_name='test-endpoint',
            catalog='main',
            schema='sales',
            user_prompt='Get all customer names',
            validate_plan=False
        )
        
        assert result['success'] is True
        assert 'sql' in result
        assert 'SELECT' in result['sql']
        assert 'customers' in result['sql']
    
    @patch('requests.post')
    def test_generate_sql_handles_timeout(self, mock_post, wizard, mock_client):
        import requests
        mock_post.side_effect = requests.exceptions.Timeout()

        with pytest.raises(InfrastructureError) as exc_info:
            wizard.generate_sql(
                endpoint_name='test-endpoint',
                catalog='main',
                schema='sales',
                user_prompt='Get all customers',
            )
        assert 'timed out' in str(exc_info.value.message).lower()


class TestForbiddenKeywords:
    """Tests for forbidden SQL keywords."""
    
    @pytest.fixture
    def mock_client(self):
        client = Mock()
        client.host = 'https://test.databricks.com'
        client.has_valid_auth.return_value = True
        return client
    
    @pytest.fixture
    def wizard(self, mock_client):
        return SQLWizardService(mock_client)
    
    @pytest.fixture
    def sample_context(self):
        return SchemaContext(
            tables=[{'name': 'test_table', 'full_name': 'main.sales.test_table', 'columns': []}]
        )
    
    @pytest.mark.parametrize("keyword", [
        'CREATE', 'ALTER', 'DROP', 'INSERT', 'UPDATE', 'DELETE',
        'TRUNCATE', 'GRANT', 'REVOKE', 'MERGE', 'REPLACE'
    ])
    def test_all_forbidden_keywords_rejected(self, wizard, sample_context, keyword):
        sql = f"SELECT * FROM test_table WHERE EXISTS (SELECT 1 WHERE 1=0 {keyword} something)"
        
        is_valid, message, _ = wizard.validate_sql_static(sql, sample_context)
        
        assert not is_valid
        assert keyword in message

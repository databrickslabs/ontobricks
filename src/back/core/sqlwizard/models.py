from typing import Dict, List, Any
from dataclasses import dataclass


@dataclass
class SchemaContext:
    """Represents the schema context for SQL generation.

    Tables are expected to have 'full_name' (catalog.schema.table) as the primary identifier.
    """

    tables: List[
        Dict[str, Any]
    ]  # [{name, full_name, columns: [{name, type, comment}]}]

    def to_yaml_like(self) -> str:
        """Render schema context in a compact YAML-like format for the LLM."""
        lines = ["tables:"]
        for table in self.tables:
            # Use full_name as the primary identifier for SQL generation
            table_display = table.get("full_name") or table.get("name", "unknown")
            lines.append(f"  - {table_display}:")
            if table.get("comment"):
                lines.append(f"    # {table['comment']}")
            for col in table.get("columns", []):
                col_str = f"      - {col['name']}: {col['type']}"
                if col.get("comment"):
                    col_str += f"  # {col['comment']}"
                lines.append(col_str)
        return "\n".join(lines)

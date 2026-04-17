"""SPARQL local execution and R2RML extraction from Turtle/XML graphs."""
import re

from rdflib import Graph, Namespace
from rdflib.namespace import RDF, RDFS

from back.core.logging import get_logger

logger = get_logger(__name__)


class SparqlQueryRunner:
    """Run SPARQL locally (RDFLib) and extract R2RML mapping structures from content."""

    @staticmethod
    def execute_local_query(query, rdf_content, limit):
        """Execute SPARQL query locally using RDFLib.

        Args:
            query: SPARQL query string
            rdf_content: RDF/Turtle content to query
            limit: Maximum number of results

        Returns:
            dict: Query results with columns and data
        """
        from back.core.w3c.rdf_utils import parse_rdf_flexible
        g = parse_rdf_flexible(rdf_content, formats=("turtle", "xml"))

        results = g.query(query)
        result_list = []
        columns = []

        if results.type == 'SELECT':
            columns = [str(var) for var in results.vars]
            for row in results:
                row_dict = {}
                for i, var in enumerate(results.vars):
                    value = row[i]
                    row_dict[str(var)] = str(value) if value else ''
                result_list.append(row_dict)
                if limit and len(result_list) >= limit:
                    break

        elif results.type in ('CONSTRUCT', 'DESCRIBE'):
            columns = ['subject', 'predicate', 'object']
            for s, p, o in results:
                result_list.append({
                    'subject': str(s),
                    'predicate': str(p),
                    'object': str(o)
                })
                if limit and len(result_list) >= limit:
                    break

        return {
            'success': True,
            'results': result_list,
            'columns': columns,
            'count': len(result_list),
            'engine': 'local'
        }

    @staticmethod
    def extract_r2rml_mappings(r2rml_content):
        """Extract mapping information from R2RML content.

        Args:
            r2rml_content: R2RML Turtle content

        Returns:
            tuple: (entity_mappings dict, relationship_mappings list)
        """
        from back.core.w3c.rdf_utils import parse_rdf_flexible
        g = parse_rdf_flexible(r2rml_content, formats=("turtle", "xml"))

        RR = Namespace('http://www.w3.org/ns/r2rml#')

        entity_mappings = {}
        relationship_mappings = []

        for tm in g.subjects(RDF.type, RR.TriplesMap):
            mapping = {
                'table': None,
                'id_column': None,
                'label_column': None,
                'uri_template': None,
                'sql_query': None,
                'predicates': {}
            }

            has_class = False

            for lt in g.objects(tm, RR.logicalTable):
                for table_name in g.objects(lt, RR.tableName):
                    mapping['table'] = str(table_name).strip('"')
                for sql_query in g.objects(lt, RR.sqlQuery):
                    mapping['sql_query'] = str(sql_query)

            subject_template = None
            subject_column = None
            for sm in g.objects(tm, RR.subjectMap):
                for template in g.objects(sm, RR.template):
                    template_str = str(template)
                    mapping['uri_template'] = template_str
                    subject_template = template_str
                    col_match = re.search(r'\{(\w+)\}', template_str)
                    if col_match:
                        mapping['id_column'] = col_match.group(1)
                        subject_column = col_match.group(1)

                for class_uri in g.objects(sm, RR['class']):
                    class_str = str(class_uri)
                    entity_mappings[class_str] = mapping
                    has_class = True

            for pom in g.objects(tm, RR.predicateObjectMap):
                predicate_uri = None
                column_name = None
                object_template = None
                object_column = None

                for pred in g.objects(pom, RR.predicate):
                    predicate_uri = str(pred)

                for om in g.objects(pom, RR.objectMap):
                    for col in g.objects(om, RR.column):
                        column_name = str(col).strip('"')

                    for template in g.objects(om, RR.template):
                        object_template = str(template)
                        col_match = re.search(r'\{(\w+)\}', object_template)
                        if col_match:
                            object_column = col_match.group(1)

                    for parent_tm in g.objects(om, RR.parentTriplesMap):
                        mapping['predicates'][predicate_uri] = {
                            'type': 'reference',
                            'parent_map': str(parent_tm)
                        }
                        for jc in g.objects(om, RR.joinCondition):
                            for child_col in g.objects(jc, RR.child):
                                mapping['predicates'][predicate_uri]['child_column'] = str(child_col).strip('"')
                            for parent_col in g.objects(jc, RR.parent):
                                mapping['predicates'][predicate_uri]['parent_column'] = str(parent_col).strip('"')

                if predicate_uri and column_name:
                    mapping['predicates'][predicate_uri] = {'type': 'column', 'column': column_name}
                    if predicate_uri == str(RDFS.label):
                        mapping['label_column'] = column_name

                if not has_class and predicate_uri and object_template and mapping.get('sql_query'):
                    relationship_mappings.append({
                        'predicate': predicate_uri,
                        'sql_query': mapping['sql_query'],
                        'subject_template': subject_template,
                        'object_template': object_template,
                        'subject_column': subject_column,
                        'object_column': object_column
                    })

        return entity_mappings, relationship_mappings

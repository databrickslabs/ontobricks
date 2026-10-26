/**
 * Read-time outgoing object-property inheritance (domain-side).
 * Mirrors OntologyClassModel.outgoing_relations_for_class.
 */
const _INHERITANCE_PRIMITIVE_RANGES = new Set([
    'string', 'integer', 'int', 'long', 'float', 'double', 'decimal',
    'boolean', 'date', 'datetime', 'time', 'duration',
    'anyuri', 'literal', 'plainliteral', 'langstring',
    'xsd:string', 'xsd:integer', 'xsd:int', 'xsd:long', 'xsd:float',
    'xsd:double', 'xsd:decimal', 'xsd:boolean', 'xsd:date',
    'xsd:datetime', 'xsd:time', 'xsd:duration', 'xsd:anyuri',
    'rdfs:literal',
]);

function _isOutgoingObjectProperty(prop) {
    const ptype = String(prop.type || '').replace(/^owl:/, '');
    if (ptype === 'DatatypeProperty') return false;
    if (ptype === 'ObjectProperty') return true;
    return !_INHERITANCE_PRIMITIVE_RANGES.has(String(prop.range || '').toLowerCase());
}

function ancestorClassNames(classes, className) {
    const byName = new Map((classes || []).filter(c => c && c.name).map(c => [c.name, c]));
    const chain = [];
    const visited = new Set([className]);
    let current = byName.get(className);
    while (current) {
        const parent = current.parent || '';
        if (!parent || visited.has(parent)) break;
        visited.add(parent);
        chain.push(parent);
        current = byName.get(parent);
    }
    return chain;
}

function outgoingRelationsForClass(classes, properties, className) {
    const declaring = new Set([className, ...ancestorClassNames(classes, className)]);
    const out = [];
    (properties || []).forEach(prop => {
        const domain = prop.domain || '';
        if (!declaring.has(domain) || !prop.range) return;
        if (!_isOutgoingObjectProperty(prop)) return;
        const inherited = domain !== className;
        out.push({
            ...prop,
            inherited,
            inheritedFrom: inherited ? domain : '',
        });
    });
    return out;
}

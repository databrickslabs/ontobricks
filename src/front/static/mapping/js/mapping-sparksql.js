/**
 * OntoBricks - mapping-sparksql.js
 * Spark SQL viewer functions — extracted from _mapping_sparksql.html per code_instructions.txt
 */

async function refreshMappingSql() {
    const sqlContent = document.getElementById('mappingSqlContent');
    const overlay = document.getElementById('sparksqlLoadingOverlay');

    try {
        if (sqlContent) sqlContent.innerHTML = '';
        if (overlay) overlay.style.display = 'flex';

        const ontologyResponse = await fetch('/ontology/get-loaded-ontology');
        const ontologyData = await ontologyResponse.json();
        const baseUri = ontologyData.ontology?.base_uri || 'http://example.org/ontology#';
        const normalizedBaseUri = baseUri.endsWith('#') || baseUri.endsWith('/') ? baseUri : baseUri + '#';

        const sparqlQuery = `PREFIX : <${normalizedBaseUri}>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?subject ?predicate ?object
WHERE {
    ?subject ?predicate ?object .
}`;

        const response = await fetch('/dtwin/translate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query: sparqlQuery })
        });

        const result = await response.json();

        if (result.success && result.sql) {
            const formattedSql = result.sql;
            if (sqlContent) sqlContent.innerHTML = `<pre style="margin: 0; padding: 1rem; white-space: pre-wrap; word-wrap: break-word; font-size: 0.85rem; font-family: 'SF Mono', 'Monaco', 'Consolas', monospace; background: #1e1e1e; color: #d4d4d4; min-height: 400px; border-radius: 4px;">${escapeHtml(formattedSql)}</pre>`;
        } else if (sqlContent) {
            sqlContent.innerHTML = `
                <div class="d-flex flex-column align-items-center justify-content-center h-100 p-4" style="color: #dc3545; min-height: 400px;">
                    <i class="bi bi-exclamation-triangle" style="font-size: 3rem;"></i>
                    <p class="mt-3 text-center">${result.message || 'Failed to generate Spark SQL'}</p>
                </div>
            `;
        }
    } catch (error) {
        console.error('Error generating Spark SQL:', error);
        if (sqlContent) sqlContent.innerHTML = `
            <div class="d-flex flex-column align-items-center justify-content-center h-100 p-4" style="color: #dc3545; min-height: 400px;">
                <i class="bi bi-exclamation-triangle" style="font-size: 3rem;"></i>
                <p class="mt-3 text-center">Error: ${error.message}</p>
            </div>
        `;
    } finally {
        if (overlay) overlay.style.display = 'none';
    }
}

function copyMappingSql() {
    const sqlContent = document.getElementById('mappingSqlContent');
    const pre = sqlContent.querySelector('pre');
    
    if (pre) {
        navigator.clipboard.writeText(pre.textContent).then(() => {
            showNotification('SQL copied to clipboard', 'success', 1500);
        }).catch(() => {
            showNotification('Failed to copy SQL', 'error');
        });
    } else {
        showNotification('No SQL to copy. Click Refresh first.', 'warning');
    }
}

// Auto-refresh when the Spark SQL section becomes visible
(function() {
    let _sparksqlLoaded = false;

    function _triggerIfReady() {
        if (_sparksqlLoaded) return;
        const section = document.getElementById('sparksql-section');
        if (section && section.classList.contains('active')) {
            _sparksqlLoaded = true;
            refreshMappingSql();
        }
    }

    document.addEventListener('sectionChange', function(e) {
        if (e.detail && e.detail.section === 'sparksql') {
            _sparksqlLoaded = false;
            refreshMappingSql();
        }
    });

    // Handle initial page load when sparksql is the default/deep-linked section
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function() {
            setTimeout(_triggerIfReady, 500);
        });
    } else {
        setTimeout(_triggerIfReady, 500);
    }
})();

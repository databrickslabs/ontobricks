/**
 * OntoBricks - query-execute.js
 * SPARQL playground runtime (samples, execution, results, SQL, CSV).
 */

const SPARQLPlayground = (() => {
    let initialized = false;
    let baseUri = "https://databricks-ontology.com/";
    let resultsGrid = null;
    let currentResult = null;

    const RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    const RDFS = "http://www.w3.org/2000/01/rdf-schema#";

    function el(id) {
        return document.getElementById(id);
    }

    function normalizedBaseUri() {
        if (!baseUri) return "https://databricks-ontology.com/";
        if (baseUri.endsWith("#") || baseUri.endsWith("/")) return baseUri;
        return `${baseUri}#`;
    }

    function readLimit() {
        const input = el("sparqlResultLimit");
        const parsed = Number.parseInt(input ? input.value : "100", 10);
        const limit = Number.isFinite(parsed) ? Math.min(5000, Math.max(1, parsed)) : 100;
        if (input) input.value = String(limit);
        return limit;
    }

    function samples(limit) {
        const ont = normalizedBaseUri();
        return {
            all: `PREFIX rdf: <${RDF}>
PREFIX rdfs: <${RDFS}>
PREFIX ont: <${ont}>

SELECT ?subject ?predicate ?object
WHERE {
  ?subject ?predicate ?object .
}
LIMIT ${limit}`,
            class: `PREFIX rdf: <${RDF}>
PREFIX ont: <${ont}>

SELECT ?subject ?predicate ?object
WHERE {
  ?subject rdf:type ont:ClassName .
  ?subject ?predicate ?object .
}
LIMIT ${limit}`,
            label: `PREFIX rdfs: <${RDFS}>

SELECT ?subject ?label
WHERE {
  ?subject rdfs:label ?label .
}
LIMIT ${limit}`,
            types: `PREFIX rdf: <${RDF}>

SELECT DISTINCT ?type
WHERE {
  ?subject rdf:type ?type .
}
LIMIT ${limit}`,
        };
    }

    function applySample(sampleKey) {
        const editor = el("sparqlPlaygroundQuery");
        if (!editor) return;
        const available = samples(readLimit());
        editor.value = available[sampleKey] || available.all;
    }

    async function loadOntologyContext() {
        const response = await fetch("/ontology/load", {
            credentials: "same-origin",
        });
        const data = await response.json().catch(() => ({}));
        const configured = data && data.config ? data.config.base_uri : "";
        if (configured) baseUri = configured;
    }

    function setStatus(kind, message) {
        const status = el("sparqlStatus");
        if (!status) return;
        status.className = `alert alert-${kind}`;
        status.textContent = message;
        status.classList.remove("d-none");
    }

    function clearStatus() {
        const status = el("sparqlStatus");
        if (!status) return;
        status.classList.add("d-none");
    }

    function updateActionState(columns, count) {
        const download = el("sparqlDownloadBtn");
        const explore = el("sparqlExploreBtn");
        if (download) download.disabled = count === 0;
        if (explore) explore.disabled = count === 0 || !isTripleProjection(columns || []);
    }

    async function init() {
        if (initialized) return;
        initialized = true;

        try {
            await loadOntologyContext();
        } catch (error) {
            console.warn("[SPARQL] Could not load ontology context:", error);
        }

        applySample("all");
        clearStatus();
        updateActionState([], 0);

        const sampleSelect = el("sparqlSample");
        if (sampleSelect) {
            sampleSelect.addEventListener("change", function (event) {
                applySample(event.target.value);
            });
        }
        const limitInput = el("sparqlResultLimit");
        if (limitInput) {
            limitInput.addEventListener("change", function () {
                readLimit();
            });
        }
        const runBtn = el("sparqlRunBtn");
        if (runBtn) runBtn.addEventListener("click", execute);
        const downloadBtn = el("sparqlDownloadBtn");
        if (downloadBtn) downloadBtn.addEventListener("click", downloadResults);
        const exploreBtn = el("sparqlExploreBtn");
        if (exploreBtn) exploreBtn.addEventListener("click", showInExplorer);
        const editor = el("sparqlPlaygroundQuery");
        if (editor) {
            editor.addEventListener("keydown", function (event) {
                if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
                    event.preventDefault();
                    execute();
                }
            });
        }
    }

    function isTripleProjection(columns) {
        const names = new Set((columns || []).map((name) => String(name).toLowerCase()));
        return (
            ["subject", "predicate", "object"].every((name) => names.has(name)) ||
            ["s", "p", "o"].every((name) => names.has(name))
        );
    }

    async function execute() {
        const editor = el("sparqlPlaygroundQuery");
        const query = editor ? editor.value.trim() : "";
        if (!query) {
            setStatus("warning", "Enter a SPARQL query.");
            return;
        }

        const button = el("sparqlRunBtn");
        if (button) {
            button.disabled = true;
            button.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Running...';
        }
        setStatus("info", "Translating SPARQL and running the generated SQL...");

        try {
            const response = await fetch("/dtwin/execute", {
                method: "POST",
                credentials: "same-origin",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ query, limit: readLimit() }),
            });
            const data = await response.json().catch(() => ({}));
            if (!response.ok || !data.success) {
                throw new Error(data.message || data.detail || `HTTP ${response.status}`);
            }

            currentResult = data;
            clearStatus();
            displayResults(data);
            displayGeneratedSql(data.generated_sql || "");

            const count = Array.isArray(data.results) ? data.results.length : 0;
            const resultCount = el("sparqlResultCount");
            if (resultCount) resultCount.textContent = String(count);
            updateActionState(data.columns || [], count);
        } catch (error) {
            currentResult = null;
            displayError(error && error.message ? error.message : "Execution failed");
            const resultCount = el("sparqlResultCount");
            if (resultCount) resultCount.textContent = "0";
            updateActionState([], 0);
        } finally {
            if (button) {
                button.disabled = false;
                button.innerHTML = '<i class="bi bi-play-fill me-1"></i>Run';
            }
        }
    }

    function displayResults(result) {
        const container = el("sparqlResultsContainer");
        if (!container) return;

        if (resultsGrid) {
            try {
                resultsGrid.destroy();
            } catch (_e) {
                // noop
            }
            resultsGrid = null;
        }

        const rows = result && Array.isArray(result.results) ? result.results : [];
        if (!rows.length) {
            container.innerHTML = [
                '<div class="results-empty-state">',
                '  <i class="bi bi-search"></i>',
                '  <p class="mb-0">No results found.</p>',
                "</div>",
            ].join("");
            return;
        }

        const columns = Array.isArray(result.columns) && result.columns.length
            ? result.columns
            : Object.keys(rows[0] || {});

        const formatCell = function (value) {
            if (value === null || value === undefined) return "";
            const str = String(value);
            if (str.startsWith("http://") || str.startsWith("https://")) {
                if (str.includes("#")) return str.split("#").pop() || str;
                return str.split("/").pop() || str;
            }
            return str;
        };

        try {
            if (typeof gridjs === "undefined" || !gridjs.Grid) {
                throw new Error("Grid.js unavailable");
            }
            const gridData = rows.map((row) => columns.map((col) => formatCell(row[col])));
            const gridConfig = {
                columns: columns.map((col) => ({ name: escapeHtml(String(col)), sort: true })),
                data: gridData,
                sort: true,
                resizable: true,
                fixedHeader: true,
                autoWidth: true,
            };
            container.innerHTML = "";
            resultsGrid = new gridjs.Grid(gridConfig).render(container);
        } catch (error) {
            renderFallbackTable(rows, columns, formatCell, container);
        }
    }

    function renderFallbackTable(rows, columns, formatCell, container) {
        let html = [
            '<div class="table-responsive">',
            '  <table class="table table-sm table-striped table-hover mb-0">',
            "    <thead><tr>",
            columns.map((col) => `<th>${escapeHtml(String(col))}</th>`).join(""),
            "    </tr></thead>",
            "    <tbody>",
        ].join("");

        rows.forEach((row) => {
            html += "<tr>";
            columns.forEach((col) => {
                html += `<td>${escapeHtml(formatCell(row[col]))}</td>`;
            });
            html += "</tr>";
        });

        html += "</tbody></table></div>";
        container.innerHTML = html;
    }

    function displayGeneratedSql(sql) {
        const target = el("sparqlGeneratedSql");
        if (!target) return;
        target.textContent = sql || "No SQL generated.";
    }

    function displayError(message) {
        const text = message || "Execution failed.";
        setStatus("danger", text);
        const container = el("sparqlResultsContainer");
        if (!container) return;
        container.innerHTML = [
            '<div class="results-empty-state">',
            '  <i class="bi bi-exclamation-triangle text-warning"></i>',
            `  <p class="mb-0">${escapeHtml(text)}</p>`,
            "</div>",
        ].join("");
    }

    function csvCell(value) {
        const text = value == null ? "" : String(value);
        return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
    }

    function downloadResults() {
        const rows = currentResult && Array.isArray(currentResult.results) ? currentResult.results : [];
        const columns = currentResult && Array.isArray(currentResult.columns) ? currentResult.columns : [];
        if (!rows.length || !columns.length) return;

        const csv = [
            columns.map(csvCell).join(","),
            ...rows.map((row) => columns.map((column) => csvCell(row[column])).join(",")),
        ].join("\n");

        const url = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
        const anchor = document.createElement("a");
        anchor.href = url;
        anchor.download = "sparql_results.csv";
        anchor.click();
        URL.revokeObjectURL(url);
    }

    function showInExplorer() {
        const rows = currentResult && Array.isArray(currentResult.results) ? currentResult.results : [];
        const columns = currentResult && Array.isArray(currentResult.columns) ? currentResult.columns : [];
        if (!rows.length || !isTripleProjection(columns)) return;

        // Switch section, then hand the rows straight to the Sigma bridge —
        // no artificial timer. `SigmaGraph.loadQueryResults` is
        // self-sufficient (loads its own libs) and claims graph-filter
        // state synchronously, so it is safe even if SigmaGraph's own
        // section-entry init runs concurrently. The click handler need not
        // await this promise chain.
        SidebarNav.switchTo("sigmagraph");
        SigmaGraph.loadQueryResults(rows, columns).then((loaded) => {
            if (!loaded && typeof showNotification === "function") {
                showNotification("Could not display these SPARQL results.", "error");
            }
        });
    }

    return {
        init,
        execute,
        applySample,
        isTripleProjection,
        showInExplorer,
    };
})();

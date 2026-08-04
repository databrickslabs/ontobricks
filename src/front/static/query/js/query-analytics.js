/* Knowledge Graph -> Analytics section.
 *
 * Paired with templates/partials/dtwin/_query_analytics.html.
 * The window.* functions at the bottom are called from query.js and
 * query-sigmagraph.js -- do not rename them without updating both.
 */
(function () {
    var _analyticsData = null;
    var _analyticsLastSections = null;
    var _charts = {};
    var _allTypes = [];   // [{ name, uri, count }] — populated from /dtwin/sync/stats
    var _jobAvailable = false;       // Databricks analytics job can run for this domain
    var _jobBlockedReason = '';      // why not, when an admin has enabled it and expects it to work

    // ------------------------------------------------------------------
    // Metric explanations — shown in popup when user clicks ? on a card
    // ------------------------------------------------------------------
    var _METRIC_INFO = {
        pagerank: {
            title: 'PageRank',
            icon: 'bi-diagram-3 text-primary',
            what: 'PageRank measures the <strong>global importance</strong> of a node based on both the quantity and quality of its connections. A node is important if many other important nodes link to it — not just if it has many connections.',
            formula: 'PR(u) = <sup>(1 − d)</sup>/<sub>N</sub> + d · Σ<sub>v → u</sub> <sup>PR(v)</sup>/<sub>deg(v)</sub>',
            formulaDesc: '<ul class="mb-0"><li><code>d = 0.85</code> — damping factor (probability of following a link vs. jumping randomly)</li><li><code>N</code> — total number of nodes in the graph</li><li><code>PR(v)</code> — PageRank of each neighbour <em>v</em> pointing to <em>u</em></li><li><code>deg(v)</code> — number of edges of neighbour <em>v</em> (it "shares" its rank equally)</li></ul>',
            example: '<strong>Scenario:</strong> In a supply-chain knowledge graph, a <em>Supplier</em> entity connected to 20 high-value <em>Product</em> entities (which themselves are connected to many <em>Order</em> entities) will accumulate a high PageRank — even if its raw degree is modest — because the rank flows from important downstream nodes.',
            why: 'Use PageRank to find the <strong>most influential entities</strong> in your knowledge graph. High-PageRank nodes are the best candidates for data-quality checks, governance reviews, or impact analysis — changes to them ripple widely.',
        },
        betweenness: {
            title: 'Betweenness Centrality',
            icon: 'bi-share text-danger',
            what: 'Betweenness centrality measures how often a node acts as a <strong>bridge</strong> on the shortest path between two other nodes. A node with high betweenness is a critical relay — information or relationships must pass through it.',
            formula: 'BC(v) = Σ<sub>s ≠ v ≠ t</sub> <sup>σ(s, t | v)</sup>/<sub>σ(s, t)</sub>',
            formulaDesc: '<ul class="mb-0"><li><code>σ(s, t)</code> — total number of shortest paths from node <em>s</em> to node <em>t</em></li><li><code>σ(s, t | v)</code> — those shortest paths that pass through <em>v</em></li><li>Result is normalised to [0, 1] by dividing by <code>(N−1)(N−2)/2</code></li></ul>',
            example: '<strong>Scenario:</strong> A <em>Department</em> entity that connects multiple <em>Employee</em> clusters to <em>Project</em> entities lies on many shortest paths. Its betweenness is high — removing it would disconnect large parts of the graph.',
            why: 'High-betweenness nodes are <strong>bottlenecks and single points of failure</strong>. They are the most critical entities for connectivity. In a knowledge graph, they often represent integration points, shared reference data, or cross-domain hubs.',
        },
        degree: {
            title: 'Degree Centrality',
            icon: 'bi-node-plus text-success',
            what: 'Degree centrality is the simplest measure: the <strong>fraction of other nodes a node is directly connected to</strong>. It captures raw activity and visibility in the local neighbourhood.',
            formula: 'DC(v) = <sup>deg(v)</sup>/<sub>(N − 1)</sub>',
            formulaDesc: '<ul class="mb-0"><li><code>deg(v)</code> — number of edges incident to node <em>v</em> (in an undirected graph)</li><li><code>N − 1</code> — maximum possible connections (normalisation factor)</li><li>Result is in [0, 1]: 1.0 means the node is connected to every other node</li></ul>',
            example: '<strong>Scenario:</strong> A <em>Customer</em> entity linked to 80 <em>Order</em>, 5 <em>Contact</em>, and 3 <em>Address</em> entities has a much higher degree than a customer with only 2 orders — it is a more active entity in the graph.',
            why: 'Use degree centrality as a quick <strong>activity indicator</strong>. High-degree nodes are likely to be the most referenced entities. In data-quality reviews, unusually high degree can flag over-linked reference records; unusually low degree may flag orphaned data.',
        },
        closeness: {
            title: 'Closeness Centrality',
            icon: 'bi-arrows-fullscreen text-info',
            what: 'Closeness centrality measures how <strong>quickly a node can reach all other nodes</strong> in the graph — the inverse of the average shortest path length to every other node.',
            formula: 'CC(v) = <sup>(N − 1)</sup>/<sub>Σ<sub>u ≠ v</sub> d(v, u)</sub>',
            formulaDesc: '<ul class="mb-0"><li><code>d(v, u)</code> — length of the shortest path between nodes <em>v</em> and <em>u</em></li><li><code>N − 1</code> — normalisation so the maximum value is 1.0</li><li>A node at the <em>centre</em> of the graph (short paths to everyone) scores close to 1.0</li></ul>',
            example: '<strong>Scenario:</strong> A <em>Product Category</em> entity that sits at the center of the graph — connected to both upstream <em>Suppliers</em> and downstream <em>Orders</em> — can reach any entity in few hops and thus has high closeness centrality.',
            why: 'High-closeness nodes are the best <strong>information brokers and coordinators</strong>. They can propagate updates, constraints, or alerts to the rest of the graph fastest. Use them to prioritise entities in synchronisation or notification workflows.',
        },
        clustering: {
            title: 'Clustering Coefficient',
            icon: 'bi-hexagon text-warning',
            what: 'The clustering coefficient measures how <strong>tightly interconnected a node\'s neighbours are</strong> — i.e. the fraction of the node\'s neighbourhood that forms triangles (closed triplets).',
            formula: 'C(v) = <sup>2 · T(v)</sup>/<sub>deg(v) · (deg(v) − 1)</sub>',
            formulaDesc: '<ul class="mb-0"><li><code>T(v)</code> — number of triangles (closed triplets) that include node <em>v</em></li><li><code>deg(v) · (deg(v) − 1) / 2</code> — maximum possible triangles for <em>v</em></li><li>Result is in [0, 1]: 1.0 means every pair of neighbours is also connected to each other</li></ul>',
            example: '<strong>Scenario:</strong> An <em>Employee</em> who works with 3 colleagues that all know each other forms a tight triangle — clustering coefficient = 1.0. An employee who works with 3 people who don\'t interact = 0.0.',
            why: 'High clustering indicates the node is embedded in a <strong>tight community or cluster</strong>. Combined with low betweenness, this identifies nodes inside silos. A node with high betweenness <em>and</em> low clustering is a classic bridge between communities — a critical integration point.',
        },
    };

    window._showMetricInfo = function (key) {
        var info = _METRIC_INFO[key];
        if (!info) return;
        var title = document.getElementById('analyticsMetricModalLabel');
        var body  = document.getElementById('analyticsMetricModalBody');
        if (title) title.innerHTML = '<i class="bi ' + info.icon + ' me-2"></i>' + info.title;
        if (body) body.innerHTML = [
            '<h6 class="fw-semibold text-secondary text-uppercase" style="font-size:0.7rem;letter-spacing:.05em">What is it?</h6>',
            '<p>' + info.what + '</p>',
            '<h6 class="fw-semibold text-secondary text-uppercase mt-3" style="font-size:0.7rem;letter-spacing:.05em">Formula</h6>',
            '<div class="bg-light border rounded p-3 mb-2 text-center" style="font-size:1.05rem">' + info.formula + '</div>',
            info.formulaDesc,
            '<h6 class="fw-semibold text-secondary text-uppercase mt-3" style="font-size:0.7rem;letter-spacing:.05em">Example</h6>',
            '<div class="alert alert-light border mb-2"><i class="bi bi-lightbulb me-1 text-warning"></i>' + info.example + '</div>',
            '<h6 class="fw-semibold text-secondary text-uppercase mt-3" style="font-size:0.7rem;letter-spacing:.05em">Why does it matter?</h6>',
            '<p class="mb-0">' + info.why + '</p>',
        ].join('\n');
        var modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('analyticsMetricModal'));
        modal.show();
    };

    // Every metric, in display order, with its presentation metadata. PageRank
    // was previously absent from this list because its tab held a table rather
    // than a chart; the dashboard charts all five.
    var _ALL_METRICS = [
        { key: 'pagerank',    label: 'PageRank',    icon: 'bi-diagram-3',         color: 'rgba(13, 110, 253, 0.75)' },
        { key: 'betweenness', label: 'Betweenness', icon: 'bi-share',             color: 'rgba(220, 53, 69, 0.75)'  },
        { key: 'degree',      label: 'Degree',      icon: 'bi-node-plus',         color: 'rgba(25, 135, 84, 0.75)'  },
        { key: 'closeness',   label: 'Closeness',   icon: 'bi-arrows-fullscreen', color: 'rgba(13, 202, 240, 0.75)' },
        { key: 'clustering',  label: 'Clustering',  icon: 'bi-hexagon',           color: 'rgba(255, 193, 7, 0.85)'  }
    ];

    var _selectedMetric = 'pagerank';
    var _distCharts = {};
    var _logScale = false;

    // Chart.js renders at 0px in a hidden pane, so charts are resized when the
    // Dashboard tab becomes visible.
    document.addEventListener('shown.bs.tab', function (e) {
        var target = e.target && e.target.getAttribute('data-bs-target');
        if (target !== '#atab-dashboard') return;
        Object.keys(_charts).forEach(function (key) {
            if (_charts[key]) _charts[key].resize();
        });
    });

    // Disable / re-enable the Run Analysis button based on whether the KG has been built
    function _setComputeBtnState(enabled, reason) {
        var btn = document.getElementById('analyticsComputeBtn');
        if (!btn) return;
        btn.disabled = !enabled;
        if (!enabled) {
            btn.title = reason || 'Build the Knowledge Graph first (KG → Sync)';
            btn.classList.add('disabled');
        } else {
            btn.title = '';
            btn.classList.remove('disabled');
        }
    }

    function _showNoGraphBanner(show) {
        var existing = document.getElementById('analyticsNoGraphBanner');
        if (show && !existing) {
            var banner = document.createElement('div');
            banner.id = 'analyticsNoGraphBanner';
            banner.className = 'alert alert-warning d-flex align-items-center gap-2 small mb-3';
            banner.innerHTML = '<i class="bi bi-exclamation-triangle-fill flex-shrink-0"></i>'
                + '<span>No Knowledge Graph has been built yet. Go to <strong>KG → Sync</strong> to build one before running analysis.</span>';
            var section = document.getElementById('analyticsSection');
            var header = section && section.querySelector('.section-header');
            if (header) header.insertAdjacentElement('afterend', banner);
        } else if (!show && existing) {
            existing.remove();
        }
    }

    // Show a banner when the Databricks analytics job is not available.
    // Differentiates two cases: toggle is off (no reason, quiet info) vs.
    // admin enabled it but something prevents it from running (warn + reason).
    function _showJobUnavailableBanner(show) {
        var existing = document.getElementById('analyticsJobUnavailableBanner');
        if (!show) {
            if (existing) existing.remove();
            return;
        }
        var cls, icon, body;
        // When the setting is already on, telling the user to turn it on is
        // worse than useless — name the prerequisite that is actually missing.
        if (_jobBlockedReason) {
            cls  = 'alert-warning';
            icon = 'bi-exclamation-triangle-fill';
            body = '<strong>Compute large-graph metrics on Databricks</strong> is enabled, '
                + 'but the job cannot run: ' + _jobBlockedReason;
        } else {
            cls  = 'alert-info';
            icon = 'bi-info-circle-fill';
            body = 'Graph Analytics runs on a Databricks job. '
                + 'Run <strong>Knowledge Graph &rarr; Build</strong> first if the graph has not been built yet, '
                + 'or enable <strong>Compute large-graph metrics on Databricks</strong> in '
                + '<strong>Settings &rarr; Global</strong> (admin only) to enable Run Analysis.';
        }
        var html = '<i class="bi ' + icon + ' flex-shrink-0 mt-1"></i><span>' + body + '</span>';
        if (existing) {
            existing.className = 'alert ' + cls + ' d-flex align-items-start gap-2 small mb-3';
            existing.innerHTML = html;
            return;
        }
        var banner = document.createElement('div');
        banner.id = 'analyticsJobUnavailableBanner';
        banner.className = 'alert ' + cls + ' d-flex align-items-start gap-2 small mb-3';
        banner.innerHTML = html;
        var section = document.getElementById('analyticsSection');
        var header = section && section.querySelector('.section-header');
        if (header) header.insertAdjacentElement('afterend', banner);
    }

    // Load entity types from the live KG stats and populate the <select>
    async function _loadEntityTypes() {
        var sel = document.getElementById('analyticsTypeSelect');
        if (!sel) return;

        // Quick check: if the graph_name is empty, no KG has been built
        var cfg = window.__TRIPLESTORE_CONFIG || {};
        if (!cfg.graph_name) {
            _setComputeBtnState(false, 'Build the Knowledge Graph first (KG → Sync)');
            _showNoGraphBanner(true);
            _showLimitInfo(false);
            sel.innerHTML = '<option value="">All types (full graph)</option>';
            return;
        }

        // Entity-type dropdown is an optional filter populated asynchronously;
        // the button state is refined after the stats fetch below.
        _showNoGraphBanner(false);

        try {
            var resp = await fetch('/dtwin/sync/stats', { credentials: 'same-origin' });
            var data = await resp.json();
            if (!data.success) {
                _setComputeBtnState(false, 'Build the Knowledge Graph first (KG → Sync)');
                _showNoGraphBanner(true);
                _showLimitInfo(false);
                sel.innerHTML = '<option value="">All types (full graph)</option>';
                return;
            }
            _allTypes = data.entity_types || [];
            _populateTypeSelect(sel);
            _showNoGraphBanner(false);

            _jobAvailable     = !!data.analytics_job_available;
            _jobBlockedReason = data.analytics_job_blocked_reason || '';
            if (!_jobAvailable) {
                _showJobUnavailableBanner(true);
                _setComputeBtnState(false,
                    _jobBlockedReason || 'Enable the Databricks analytics job in Settings → Global first');
            } else {
                _showJobUnavailableBanner(false);
                _setComputeBtnState(true);
            }
        } catch (e) {
            sel.innerHTML = '<option value="">All types (full graph)</option>';
        }
    }

    function _populateTypeSelect(sel) {
        var prev = sel.value;   // remember current selection across reloads
        sel.innerHTML = '<option value="">All types (full graph)</option>';
        _allTypes.forEach(function (t) {
            var uri   = t.uri || t.name;
            var label = (t.name || _localName(uri)) + (t.count ? '  (' + t.count + ')' : '');
            var opt   = document.createElement('option');
            opt.value = uri;
            opt.textContent = label;
            opt.title = uri;
            sel.appendChild(opt);
        });
        // Restore previous selection if still valid
        if (prev && Array.from(sel.options).some(function (o) { return o.value === prev; })) {
            sel.value = prev;
        }
    }

    function _getSelectedTypes() {
        var sel = document.getElementById('analyticsTypeSelect');
        if (!sel || !sel.value) return null;   // "" = All = no filter
        return [sel.value];
    }

    // Reset the AI Insights / health cards to their pristine prompt state.
    function _resetAnalyticsCards() {
        var _hc = document.getElementById('analyticsHealthCard');
        if (_hc) _hc.classList.add('d-none');
        var _he = document.getElementById('analyticsHealthEmpty');
        if (_he) _he.classList.add('d-none');
        var _ib = document.getElementById('analyticsInsightsBody');
        if (_ib) _ib.innerHTML = '<div class="text-center text-muted small py-5">'
            + '<i class="bi bi-stars fs-3 d-block mb-2 text-warning-emphasis"></i>'
            + 'Click <strong>Interpret</strong> in the toolbar to generate AI insights.</div>';
        var _is = document.getElementById('analyticsInsightsSpinner');
        if (_is) _is.classList.add('d-none');
        var _ias = document.getElementById('analyticsInsightsStatus');
        if (_ias) _ias.textContent = '';
    }

    // Format an ISO timestamp into a short "x ago" / locale string.
    function _formatComputedAt(iso) {
        if (!iso) return '';
        var d = new Date(iso);
        if (isNaN(d.getTime())) return '';
        var diffSec = Math.floor((Date.now() - d.getTime()) / 1000);
        if (diffSec < 60) return 'just now';
        if (diffSec < 3600) return Math.floor(diffSec / 60) + 'm ago';
        if (diffSec < 86400) return Math.floor(diffSec / 3600) + 'h ago';
        return d.toLocaleString();
    }

    // Render a metrics payload (from compute OR from the stored cache) into
    // the stat cards, charts and type profiles. ``meta`` carries provenance
    // for the stored-result path (computed_at, class_filter).
    function _renderAnalyticsData(data, meta) {
        meta = meta || {};
        _analyticsData = data;

        // Restore the entity-type filter selection from a stored result so
        // the subtitle and re-runs reflect what was actually computed.
        var _selEl = document.getElementById('analyticsTypeSelect');
        if (meta.class_filter && meta.class_filter.length && _selEl) {
            var wanted = meta.class_filter[0];
            if (Array.from(_selEl.options).some(function (o) { return o.value === wanted; })) {
                _selEl.value = wanted;
            }
        }

        // Update subtitle with the active filter
        var _subtitle    = document.getElementById('analyticsSubtitle');
        var _subtitleTxt = document.getElementById('analyticsSubtitleText');
        var _selOpt = _selEl && _selEl.selectedIndex >= 0 ? _selEl.options[_selEl.selectedIndex] : null;
        if (_subtitle && _subtitleTxt) {
            if (_selOpt && _selEl.value) {
                _subtitleTxt.textContent = _selOpt.text.trim();
                _subtitle.classList.remove('d-none');
            } else {
                _subtitle.classList.add('d-none');
            }
        }

        // "Last computed" provenance line
        var _ca    = document.getElementById('analyticsComputedAt');
        var _caTxt = document.getElementById('analyticsComputedAtText');
        if (_ca && _caTxt) {
            var when = _formatComputedAt(meta.computed_at);
            if (when) {
                var how = data.mode === 'job' ? ' (computed on Databricks)' : '';
                _caTxt.textContent = 'Last computed ' + when + how;
                _ca.classList.remove('d-none');
            } else {
                _ca.classList.add('d-none');
            }
        }

        var s = data.stats || {};
        _setText('aStatNodes',      s.node_count        != null ? s.node_count.toLocaleString()  : '—');
        _setText('aStatEdges',      s.edge_count        != null ? s.edge_count.toLocaleString()  : '—');
        _setText('aStatComponents', s.connected_components != null ? s.connected_components      : '—');
        _setText('aStatAvgDegree',  s.avg_degree        != null ? s.avg_degree.toFixed(2)        : '—');
        _setText('aStatDensity',    s.density           != null ? s.density.toFixed(6)           : '—');
        _setText('aStatElapsed',    s.elapsed_ms        != null ? s.elapsed_ms + ' ms'           : '—');

        // Show graph_node_count sub-label when filter was applied
        var graphNodeEl = document.getElementById('aStatGraphNodes');
        if (graphNodeEl) {
            var gn = s.graph_node_count;
            graphNodeEl.textContent = (gn && gn !== s.node_count) ? '(' + gn.toLocaleString() + ' in subgraph)' : '';
        }

        _renderDistributionStrip();
        analyticsRenderCharts();
        _renderTypeProfiles(data.entity_type_profiles, !!_getSelectedTypes());

        var results = document.getElementById('analyticsResults');
        if (results) results.classList.remove('d-none');

        // Reveal the Interpret button; reset audit button
        var interpretBtn = document.getElementById('analyticsInterpretBtn');
        if (interpretBtn) interpretBtn.classList.remove('d-none');
        var auditBtn = document.getElementById('analyticsAuditBtn');
        if (auditBtn) auditBtn.classList.add('d-none');
        _analyticsLastSections = null;
    }

    // Load and render the LAST persisted analytics result for this domain.
    // Returns true when a stored result was rendered. Shows the spinner while
    // the stored result is being fetched/rendered (e.g. on section open).
    window.analyticsLoadLatest = async function () {
        var spinner = document.getElementById('analyticsSpinner');
        var spinnerMsg = spinner ? spinner.querySelector('p') : null;
        var prevMsg = spinnerMsg ? spinnerMsg.textContent : '';
        if (spinnerMsg) spinnerMsg.textContent = 'Loading the last analysis…';
        if (spinner) spinner.classList.remove('d-none');
        try {
            var resp = await fetch('/dtwin/metrics/latest', { credentials: 'same-origin' });
            var data = await resp.json();
            if (!data.success || !data.has_result) return false;
            _renderAnalyticsData(data, {
                computed_at: data.computed_at,
                class_filter: data.class_filter
            });
            return true;
        } catch (e) {
            return false;
        } finally {
            if (spinner) spinner.classList.add('d-none');
            if (spinnerMsg) spinnerMsg.textContent = prevMsg;
        }
    };

    // Toggle the "analysis in progress" UI: spinner on/off + button disabled.
    function _setAnalysisBusy(busy) {
        var btn     = document.getElementById('analyticsComputeBtn');
        var spinner = document.getElementById('analyticsSpinner');
        if (btn) btn.disabled = !!busy;
        if (spinner) spinner.classList.toggle('d-none', !busy);
    }

    // Find an in-flight graph-analytics background task, if any. Lets the
    // Analytics page resume its spinner after the user navigates away and back
    // while a computation is still running in the TaskManager.
    async function _findRunningAnalyticsTask() {
        try {
            var resp = await fetch('/tasks/?include_completed=false', { credentials: 'same-origin' });
            var data = await resp.json();
            if (!data.success) return null;
            var tasks = data.tasks || [];
            for (var i = 0; i < tasks.length; i++) {
                var t = tasks[i];
                if (t.task_type === 'graph_analytics'
                    && (t.status === 'running' || t.status === 'pending')) {
                    return t;
                }
            }
            return null;
        } catch (e) {
            return null;
        }
    }

    // Wait for an already-running analysis task to finish, then load its result.
    async function _waitAndLoad(taskId) {
        _setAnalysisBusy(true);
        try {
            await waitForTask(taskId);
            var ok = await window.analyticsLoadLatest();
            if (!ok) _showAnalyticsError('Analysis finished but no result could be loaded');
        } catch (e) {
            _showAnalyticsError('Analysis failed: ' + (e && e.message ? e.message : e));
        } finally {
            _setAnalysisBusy(false);
        }
    }

    // Called on section open. ALWAYS render the last stored run first so the
    // PageRank / Betweenness / … tabs are populated immediately, independent of
    // any in-flight task. Then, if a computation is still running in the
    // background, re-show the spinner and resume waiting (the last result stays
    // visible underneath until the new one lands).
    window.analyticsResume = async function () {
        await window.analyticsLoadLatest();
        var running = await _findRunningAnalyticsTask();
        if (running) {
            await _waitAndLoad(running.id);
        }
    };

    window.analyticsCompute = async function () {
        // Backstop guard: the button is already disabled when the job is
        // unavailable, but direct JS calls also need defending.
        if (!_jobAvailable) {
            _showAnalyticsError(_jobBlockedReason
                ? 'The analytics job cannot run: ' + _jobBlockedReason
                : 'Enable the Databricks analytics job in Settings → Global first.');
            return;
        }

        _setAnalysisBusy(true);

        _resetAnalyticsCards();

        // Ensure entity types are loaded (fallback if section was opened directly)
        if (_allTypes.length === 0) _loadEntityTypes();

        try {
            var resp = await fetch('/dtwin/metrics/compute', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ class_filter: _getSelectedTypes() })
            });
            var data = await resp.json();
            if (!data.success || !data.task_id) {
                _showAnalyticsError(data.message || 'Could not start the analysis');
                return;
            }

            // Refresh the global task tracker so the new job shows immediately.
            if (typeof refreshTasks === 'function') refreshTasks();

            // Wait for the background task to finish, then render from storage.
            await waitForTask(data.task_id);

            var ok = await window.analyticsLoadLatest();
            if (!ok) {
                _showAnalyticsError('Analysis finished but no result could be loaded');
                return;
            }

            if (typeof showNotification === 'function') {
                // Read the count from stats, never from Object.keys(nodes): the
                // nodes dict is a bounded top-N slice, not the full graph.
                var st = (_analyticsData && _analyticsData.stats) || {};
                var n  = st.node_count || 0;
                var ms = st.elapsed_ms;
                showNotification('Analysis done: ' + n.toLocaleString() + ' nodes'
                    + (ms ? ' in ' + ms + 'ms' : ''), 'success');
            }

        } catch (e) {
            _showAnalyticsError('Error: ' + (e && e.message ? e.message : e));
        } finally {
            _setAnalysisBusy(false);
        }
    };

    // Navigate to the Graph Viewer and filter on a specific entity URI
    function _navigateToGraph(uri) {
        var link = document.querySelector('[data-section="sigmagraph"]');
        if (!link) return;
        link.click();
        var localName = _localName(uri) || uri;
        setTimeout(function () {
            var valInput = document.getElementById('sgFilterValue');
            if (valInput) {
                valInput.value = localName;
                valInput.dispatchEvent(new Event('input', { bubbles: true }));
            }
            if (typeof SigmaGraph !== 'undefined' && SigmaGraph.executeGraphFilter) {
                SigmaGraph.executeGraphFilter();
            }
        }, 400);
    }

    window.analyticsRenderCharts = function () {
        if (!_analyticsData) return;
        _renderRankingChart();
        var topN = _topN();
        _renderPagerankTable(
            Object.keys(_analyticsData.nodes || {}),
            _analyticsData.nodes || {},
            _analyticsData.node_types || {},
            topN
        );
    };

    function _topN() {
        var el = document.getElementById('analyticsTopN');
        return Math.max(3, parseInt(el && el.value, 10) || 10);
    }

    function _renderRankingChart() {
        var host = document.getElementById('analyticsRankingCard');
        if (!host || !_analyticsData) return;

        var meta = _ALL_METRICS.filter(function (m) {
            return m.key === _selectedMetric;
        })[0] || _ALL_METRICS[0];

        var unavailable = _analyticsData.unavailable_metrics || [];
        var approximate = _analyticsData.approximate_metrics || [];
        var pivotCount = _analyticsData.pivot_count || 0;

        var segments = _ALL_METRICS.map(function (m) {
            return '<button type="button" class="analytics-rank-seg'
                + (m.key === _selectedMetric ? ' selected' : '') + '"'
                + ' id="rankSeg_' + m.key + '" data-metric="' + m.key + '"'
                + ' aria-pressed="' + (m.key === _selectedMetric) + '">'
                + '<i class="bi ' + m.icon + ' me-1"></i>' + m.label
                + '</button>';
        }).join('');

        // Must destroy while the ranking canvas is still attached to the DOM.
        if (_charts.ranking) { _charts.ranking.destroy(); _charts.ranking = null; }

        host.innerHTML = ''
            + '<div class="card mt-2">'
            + '  <div class="card-header py-2 d-flex justify-content-between align-items-center flex-wrap gap-2">'
            + '    <span class="small fw-semibold">'
            + '      <i class="bi ' + meta.icon + ' me-1"></i>Top nodes by ' + meta.label
            + '      <button class="btn btn-link btn-sm p-0 text-muted ms-1"'
            + '              onclick="_showMetricInfo(\'' + meta.key + '\')"'
            + '              title="What is ' + meta.label + '?">'
            + '        <i class="bi bi-question-circle"></i></button>'
            + '    </span>'
            + '    <span class="analytics-rank-segs">' + segments + '</span>'
            + '  </div>'
            + '  <div class="card-body">'
            + '    <div id="analyticsRankingNotice"></div>'
            + '    <div class="analytics-rank-canvas-wrap">'
            + '      <canvas id="analyticsRankingChart"></canvas>'
            + '    </div>'
            + '  </div>'
            + '</div>';

        host.querySelectorAll('.analytics-rank-seg').forEach(function (el) {
            el.addEventListener('click', function () {
                _selectMetric(el.getAttribute('data-metric'));
            });
        });

        var notice = document.getElementById('analyticsRankingNotice');
        var canvas = document.getElementById('analyticsRankingChart');
        if (!canvas || !notice) return;

        var allNodes = _analyticsData.nodes || {};
        var sorted = Object.keys(allNodes).sort(function (a, b) {
            return (allNodes[b][meta.key] || 0) - (allNodes[a][meta.key] || 0);
        }).slice(0, _topN());
        var values = sorted.map(function (uri) {
            return +(allNodes[uri][meta.key] || 0).toFixed(6);
        });

        // A flat zero chart would imply a measurement of zero. Explain instead.
        if (!values.length || values.every(function (v) { return v === 0; })) {
            canvas.style.display = 'none';
            notice.innerHTML = '<div class="alert alert-light border small text-muted mb-0">'
                + '<i class="bi bi-info-circle me-1"></i>'
                + _zeroReason(meta.key, unavailable) + '</div>';
            return;
        }
        canvas.style.display = '';

        notice.innerHTML = approximate.indexOf(meta.key) !== -1
            ? '<div class="alert alert-warning border small py-1 px-2 mb-2">'
              + '<i class="bi bi-exclamation-triangle me-1"></i><strong>Estimate.</strong> '
              + meta.label + ' is sampled from ' + pivotCount + ' source node'
              + (pivotCount === 1 ? '' : 's') + ' rather than all of them, because the '
              + 'exact computation is quadratic in the graph size. Use it to rank nodes, '
              + 'not as an absolute value — nodes with similar scores may be ordered '
              + 'wrongly. Analyse a single Entity Type for exact values.</div>'
            : '';

        var labels = sorted.map(_displayName);
        _charts.ranking = new Chart(canvas, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: meta.label,
                    data: values,
                    backgroundColor: meta.color,
                    borderRadius: 4,
                    borderSkipped: false
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                onClick: function (event, elements) {
                    if (!elements || !elements.length) return;
                    var uri = sorted[elements[0].index];
                    if (uri) _navigateToGraph(uri);
                },
                onHover: function (event) {
                    event.native.target.style.cursor =
                        event.chart.getElementsAtEventForMode(
                            event.native, 'nearest', { intersect: true }, true
                        ).length ? 'pointer' : 'default';
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            title: function (items) {
                                var uri = sorted[items[0].dataIndex];
                                var type = (_analyticsData.node_types || {})[uri];
                                return type ? uri + '  [' + _localName(type) + ']' : uri;
                            },
                            beforeBody: function (items) {
                                var nm = allNodes[sorted[items[0].dataIndex]] || {};
                                return _ALL_METRICS.map(function (m) {
                                    return m.label + ' : ' + (nm[m.key] || 0).toFixed(6);
                                }).concat(['──────────────────────────']);
                            },
                            label: function (item) {
                                return '► ' + item.dataset.label + ' : ' + item.formattedValue;
                            },
                            afterLabel: function () { return '\nClick to open in Graph Viewer'; }
                        }
                    }
                },
                scales: {
                    x: { beginAtZero: true, ticks: { font: { size: 11 } },
                         grid: { color: 'rgba(0,0,0,0.05)' } },
                    y: { ticks: { font: { size: 11 },
                                  callback: function (val, idx) {
                                      var l = labels[idx];
                                      return l.length > 40 ? l.slice(0, 39) + '…' : l;
                                  } },
                         grid: { display: false } }
                }
            }
        });
    }

    // Why a metric charts as all zeros. Kept as one function so the three
    // explanations cannot drift apart.
    function _zeroReason(key, unavailable) {
        var label = key.charAt(0).toUpperCase() + key.slice(1);
        if (unavailable.indexOf(key) !== -1) {
            return '<strong>Not computed for this graph.</strong> ' + label
                + ' is estimated from a sample of source nodes, and this run could '
                + 'not produce a sample it can stand behind — either no pivots were '
                + 'sampled or the breadth-first search hit its depth cap. '
                + 'Raise the analytics job\'s max depth in Settings and re-run.';
        }
        if (key === 'clustering') {
            return '<strong>All values are 0.</strong> Clustering coefficient is 0 '
                + 'when none of a node\'s neighbors are connected to each other — '
                + 'typical for KGs with a bipartite structure (e.g. Customer → Order '
                + '→ Product). Triangles are rare unless entities of the same type '
                + 'link directly.';
        }
        return '<strong>All values are 0.</strong> No ' + key + ' scores could be '
            + 'computed for the current graph / filter.';
    }

    // Explain what the table is ranked by and which columns are missing or
    // sampled, so the numbers cannot be read as more precise than they are.
    function _setPagerankTableNote(rankBy, unavailable, approximate) {
        var note = document.getElementById('pagerankTableNote');
        if (!note) return;
        var parts = [];
        if (rankBy !== 'pagerank') {
            parts.push('Ranked by <strong>degree</strong>, because PageRank was not '
                + 'computed in this mode.');
        }
        if (unavailable.length) {
            parts.push('<strong>&mdash;</strong> marks a metric this run did not compute ('
                + unavailable.join(', ') + ').');
        }
        if (approximate.length) {
            parts.push('<strong>&asymp;</strong> marks a sampled estimate ('
                + approximate.join(', ') + ') &mdash; reliable for ranking the '
                + 'clear leaders, not as an absolute value.');
        }
        if (!parts.length) {
            note.classList.add('d-none');
            note.innerHTML = '';
            return;
        }
        note.classList.remove('d-none');
        note.innerHTML = '<i class="bi bi-info-circle me-1"></i>' + parts.join(' ');
    }

    function _renderPagerankTable(candidateUris, allNodes, nodeTypes, topN) {
        var tbody = document.getElementById('pagerankDetailBody');
        if (!tbody) return;

        var unavailable = (_analyticsData && _analyticsData.unavailable_metrics) || [];
        var approximate = (_analyticsData && _analyticsData.approximate_metrics) || [];

        // Rank by PageRank only when it was actually computed; fall back to
        // degree when PageRank is in unavailable_metrics to avoid an arbitrary
        // order under a header that implies a ranking.
        var rankBy = unavailable.indexOf('pagerank') === -1 ? 'pagerank' : 'degree';
        var sorted = candidateUris
            .slice()
            .sort(function (a, b) { return (allNodes[b][rankBy] || 0) - (allNodes[a][rankBy] || 0); })
            .slice(0, topN);
        _setPagerankTableNote(rankBy, unavailable, approximate);

        // Per-metric max values for relative bar widths
        var metrics = ['pagerank', 'degree', 'betweenness', 'closeness', 'clustering'];
        var maxVal = {};
        metrics.forEach(function (k) {
            maxVal[k] = Math.max.apply(null, sorted.map(function (u) { return allNodes[u][k] || 0; })) || 1;
        });

        var colors = {
            pagerank:    'var(--bs-primary)',
            degree:      'var(--bs-success)',
            betweenness: 'var(--bs-danger)',
            closeness:   'var(--bs-info)',
            clustering:  'var(--bs-warning)',
        };

        tbody.innerHTML = '';
        sorted.forEach(function (uri, rank) {
            var nm   = allNodes[uri];
            var type = nodeTypes[uri];
            var typeSuffix = type ? '<span class="badge bg-light border text-secondary ms-1" style="font-size:0.7rem">' + _localName(type) + '</span>' : '';
            var displayName = _displayName(uri);

            function cell(key) {
                // A metric this run could not compute is stored as 0. Printing
                // "0.0000" would read as a real measurement, so show a dash
                // instead — the charts already explain why it is missing.
                if (unavailable.indexOf(key) !== -1) {
                    return '<td class="text-end text-muted" style="min-width:90px"'
                        + ' title="Not computed in this mode — see the chart above">'
                        + '<div style="font-size:0.78rem">&mdash;</div></td>';
                }
                var v   = nm[key] || 0;
                var pct = Math.round((v / maxVal[key]) * 100);
                // "≈" marks a sampled estimate so a reader comparing this column
                // to an exact one knows the difference.
                var prefix = approximate.indexOf(key) !== -1 ? '&asymp;&thinsp;' : '';
                return '<td class="text-end" style="min-width:90px">'
                    + '<div style="font-size:0.78rem;font-variant-numeric:tabular-nums">'
                    + prefix + v.toFixed(4) + '</div>'
                    + '<div style="height:3px;border-radius:2px;background:' + colors[key] + ';width:' + pct + '%;margin-left:auto"></div>'
                    + '</td>';
            }

            var row = '<tr style="cursor:pointer" onclick="_analyticsDrillURI(\'' + uri.replace(/'/g, "\\'") + '\')" title="Open in Graph Viewer">'
                + '<td class="text-center text-muted small">' + (rank + 1) + '</td>'
                + '<td><span class="fw-semibold small">' + displayName + '</span>' + typeSuffix + '</td>'
                + cell('pagerank')
                + cell('degree')
                + cell('betweenness')
                + cell('closeness')
                + cell('clustering')
                + '</tr>';
            tbody.insertAdjacentHTML('beforeend', row);
        });
    }

    window._analyticsDrillURI = function (uri) { _navigateToGraph(uri); };

    // Format a metric value compactly enough for a tile caption.
    function _fmtMetric(v) {
        if (v === 0) return '0';
        if (v == null || isNaN(v)) return '—';
        return Math.abs(v) < 0.001 ? v.toExponential(1) : v.toFixed(4);
    }

    function _renderDistributionStrip() {
        var host = document.getElementById('analyticsDistStrip');
        if (!host) return;

        var dists = (_analyticsData && _analyticsData.distributions) || {};
        var approximate = (_analyticsData && _analyticsData.approximate_metrics) || [];
        var unavailable = (_analyticsData && _analyticsData.unavailable_metrics) || [];

        Object.keys(_distCharts).forEach(function (k) {
            if (_distCharts[k]) _distCharts[k].destroy();
        });
        _distCharts = {};

        host.innerHTML = _ALL_METRICS.map(function (m) {
            var isApprox = approximate.indexOf(m.key) !== -1;
            var badge = isApprox
                ? '<span class="analytics-dist-badge" title="Sampled estimate">&asymp;</span>'
                : '';
            return ''
                + '<button type="button" class="analytics-dist-tile'
                + (m.key === _selectedMetric ? ' selected' : '') + '"'
                + ' id="distTile_' + m.key + '"'
                + ' data-metric="' + m.key + '"'
                + ' aria-pressed="' + (m.key === _selectedMetric) + '"'
                + ' title="Show the top-ranked nodes by ' + m.label + '">'
                + '  <span class="analytics-dist-head">'
                + '    <i class="bi ' + m.icon + '"></i>' + m.label + badge
                + '  </span>'
                + '  <span class="analytics-dist-body">'
                + '    <canvas id="distChart_' + m.key + '"></canvas>'
                + '  </span>'
                + '  <span class="analytics-dist-caption" id="distCaption_' + m.key + '"></span>'
                + '</button>';
        }).join('');

        host.querySelectorAll('.analytics-dist-tile').forEach(function (el) {
            el.addEventListener('click', function () {
                _selectMetric(el.getAttribute('data-metric'));
            });
        });

        _ALL_METRICS.forEach(function (m) {
            var dist = dists[m.key];
            var caption = document.getElementById('distCaption_' + m.key);
            var canvas = document.getElementById('distChart_' + m.key);
            if (!canvas || !caption) return;

            // A metric the run could not compute is stored as zeros, and a
            // legacy cached result predates distributions entirely. Neither may
            // be drawn as a chart.
            if (!dist || !dist.bins || !dist.bins.length) {
                canvas.style.display = 'none';
                caption.innerHTML = unavailable.indexOf(m.key) !== -1
                    ? '<i class="bi bi-dash-circle me-1"></i>Not computed for this run'
                    : '<i class="bi bi-info-circle me-1"></i>Re-run the analysis to see the distribution';
                return;
            }
            canvas.style.display = '';

            caption.innerHTML = 'median &asymp; ' + _fmtMetric(dist.median)
                + ' &middot; max ' + _fmtMetric(dist.hi)
                + (_logScale ? ' &middot; <em>log</em>' : '');

            var width = (dist.hi - dist.lo) / dist.bins.length;
            _distCharts[m.key] = new Chart(canvas, {
                type: 'bar',
                data: {
                    labels: dist.bins.map(function (_, i) {
                        return _fmtMetric(dist.lo + i * width);
                    }),
                    datasets: [{
                        data: dist.bins,
                        backgroundColor: m.color,
                        borderRadius: 1,
                        borderSkipped: false
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    animation: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                title: function (items) {
                                    var i = items[0].dataIndex;
                                    return _fmtMetric(dist.lo + i * width) + ' – '
                                        + _fmtMetric(dist.lo + (i + 1) * width);
                                },
                                label: function (item) {
                                    return item.parsed.y.toLocaleString() + ' nodes';
                                }
                            }
                        }
                    },
                    scales: {
                        x: { display: false, grid: { display: false } },
                        y: {
                            display: false,
                            type: _logScale ? 'logarithmic' : 'linear',
                            beginAtZero: !_logScale
                        }
                    }
                }
            });
        });
    }

    function _selectMetric(key) {
        if (!key) return;
        _selectedMetric = key;
        document.querySelectorAll('.analytics-dist-tile').forEach(function (el) {
            var on = el.getAttribute('data-metric') === key;
            el.classList.toggle('selected', on);
            el.setAttribute('aria-pressed', String(on));
        });
        _renderRankingChart();
    }

    function _renderTypeProfiles(profiles, hasFilter) {
        var card = document.getElementById('analyticsHealthCard');
        var tbody = document.getElementById('analyticsHealthBody');
        if (!card || !tbody) return;

        var emptyEl = document.getElementById('analyticsHealthEmpty');
        // Hide when profiles are empty
        if (!profiles || !Object.keys(profiles).length) {
            card.classList.add('d-none');
            if (emptyEl) emptyEl.classList.remove('d-none');
            return;
        }
        if (emptyEl) emptyEl.classList.add('d-none');

        var rows = Object.values(profiles).sort(function (a, b) {
            // Flat types first, then by count descending
            if (a.is_flat !== b.is_flat) return a.is_flat ? -1 : 1;
            return (b.count || 0) - (a.count || 0);
        });

        tbody.innerHTML = '';
        rows.forEach(function (p) {
            var typeName = _localName(p.uri);
            var countStr = (p.count || 0).toLocaleString();
            var degStr   = (p.avg_degree || 0).toFixed(4);
            var predStr  = String(p.distinct_predicates || 0);

            var statusBadge = p.is_flat
                ? '<span class="badge bg-warning-subtle text-warning-emphasis border border-warning me-1" title="This entity type has structural patterns typical of a flat dataset or time series">'
                  + '<i class="bi bi-exclamation-triangle me-1"></i>flat</span>'
                : '<span class="badge bg-success-subtle text-success-emphasis border border-success me-1">'
                  + '<i class="bi bi-check-circle me-1"></i>graph</span>';

            var reasonsBadges = '';
            if (p.is_flat && p.flat_reasons && p.flat_reasons.length) {
                reasonsBadges = p.flat_reasons.map(function (r) {
                    return '<span class="badge bg-light border text-secondary ms-1" style="font-size:0.7rem;font-weight:400">' + _esc(r) + '</span>';
                }).join('');
            }

            if (p.has_temporal_predicates && !p.is_flat) {
                reasonsBadges += '<span class="badge bg-info-subtle text-info-emphasis border border-info ms-1" style="font-size:0.7rem">'
                    + '<i class="bi bi-clock me-1"></i>temporal</span>';
            }

            tbody.insertAdjacentHTML('beforeend',
                '<tr>'
                + '<td class="ps-3 fw-semibold">' + _esc(typeName) + '<span class="text-muted fw-normal ms-1" style="font-size:0.72rem">' + _esc(p.uri) + '</span></td>'
                + '<td class="text-end">' + countStr + '</td>'
                + '<td class="text-end font-monospace">' + degStr + '</td>'
                + '<td class="text-end">' + predStr + '</td>'
                + '<td>' + statusBadge + reasonsBadges + '</td>'
                + '</tr>'
            );
        });

        card.classList.remove('d-none');
    }

    function _showAnalyticsError(msg) {
        var noData = document.getElementById('analyticsNoData');
        if (noData) {
            noData.innerHTML = '<div class="alert alert-danger small mb-3"><i class="bi bi-exclamation-triangle me-1"></i>' + msg + '</div>';
            noData.classList.remove('d-none');
        }
        var spinner = document.getElementById('analyticsSpinner');
        if (spinner) spinner.classList.add('d-none');
        var btn = document.getElementById('analyticsComputeBtn');
        if (btn) btn.disabled = false;
    }

    function _setText(id, val) { var el = document.getElementById(id); if (el) el.textContent = val; }


    // Exposed so query.js can populate the dropdown on section activation.
    // Also hydrates the page from the LAST stored analytics result (if any) —
    // or, when a computation is still running in the background, re-shows the
    // spinner and resumes waiting (so navigating away and back keeps progress).
    window.analyticsLoadTypes = function () {
        _loadEntityTypes().then(function () {
            window.analyticsResume();
        }).catch(function () {
            window.analyticsResume();
        });
    };

    // Run an immediate guard on load: if the config already tells us there is no graph,
    // disable the button right away without waiting for an API round-trip.
    (function _initialGuard() {
        var cfg = window.__TRIPLESTORE_CONFIG || {};
        if (!cfg.graph_name) {
            _setComputeBtnState(false, 'Build the Knowledge Graph first (KG → Sync)');
        }
    })();

    // Applies to all five distribution tiles at once. Per-visit state: a
    // persisted axis choice would silently change how a colleague reads a
    // shared screenshot.
    window.analyticsToggleLogScale = function analyticsToggleLogScale() {
        var el = document.getElementById('analyticsLogScale');
        _logScale = !!(el && el.checked);
        _renderDistributionStrip();
    };

    window.analyticsInterpret = async function () {
        if (!_analyticsData) return;

        var btn    = document.getElementById('analyticsInterpretBtn');
        var body   = document.getElementById('analyticsInsightsBody');
        var status = document.getElementById('analyticsInsightsStatus');
        var spinner = document.getElementById('analyticsInsightsSpinner');

        // Switch to the AI Insights tab so user sees progress immediately
        var insightsTabBtn = document.getElementById('atab-btn-insights');
        if (insightsTabBtn) bootstrap.Tab.getOrCreateInstance(insightsTabBtn).show();

        if (btn) btn.disabled = true;
        if (body) body.innerHTML = '';
        if (spinner) spinner.classList.remove('d-none');
        if (status) status.textContent = '';

        try {
            var payload = Object.assign({}, _analyticsData, { class_filter: _getSelectedTypes() });
            var resp = await fetch('/dtwin/metrics/interpret', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify(payload)
            });
            var result = await resp.json();

            if (!result.success || !result.sections) {
                throw new Error(result.message || 'Interpretation failed');
            }

            // Section meta: icon, accent colour, badge style
            var _SEC_META = {
                'Key Findings':     { icon: 'bi-lightbulb',        color: 'text-warning',  badge: 'bg-warning-subtle text-warning-emphasis' },
                'Notable Entities': { icon: 'bi-person-badge',     color: 'text-primary',  badge: 'bg-primary-subtle text-primary-emphasis' },
                'Recommendations':  { icon: 'bi-check2-circle',    color: 'text-success',  badge: 'bg-success-subtle text-success-emphasis' },
                'Ontology Modeling':{ icon: 'bi-diagram-3',        color: 'text-info',     badge: 'bg-info-subtle text-info-emphasis' },
            };

            var html = '<div class="row g-3">';
            result.sections.forEach(function (sec) {
                var meta = _SEC_META[sec.title] || { icon: 'bi-info-circle', color: 'text-secondary', badge: 'bg-secondary-subtle text-secondary-emphasis' };
                html += '<div class="col-12">';
                html += '<div class="card border-0 bg-light h-100">';
                html += '<div class="card-body p-3">';
                // Section title with icon badge
                html += '<div class="d-flex align-items-center gap-2 mb-2">'
                    + '<span class="badge ' + meta.badge + ' px-2 py-1" style="font-size:0.78rem">'
                    + '<i class="bi ' + meta.icon + ' me-1"></i>' + _esc(sec.title)
                    + '</span></div>';

                if (sec.body) {
                    // Convert basic markdown: **bold**, *italic*, line-breaks
                    html += '<p class="mb-0 small lh-lg">' + _md(sec.body) + '</p>';
                } else if (Array.isArray(sec.items)) {
                    html += '<ul class="mb-0 ps-3 small lh-lg">';
                    sec.items.forEach(function (item) {
                        html += '<li class="mb-1">';
                        if (typeof item === 'string') {
                            html += _md(item);
                        } else if (item.label) {
                            var nodeUri = _findUriByLabel(item.label);
                            if (nodeUri) {
                                html += '<a href="#" class="fw-semibold text-decoration-none" onclick="_analyticsDrillURI('
                                    + JSON.stringify(nodeUri) + ');return false;">'
                                    + _esc(item.label) + '</a>';
                            } else {
                                html += '<strong>' + _esc(item.label) + '</strong>';
                            }
                            if (item.reason) html += '<span class="text-muted"> — ' + _md(item.reason) + '</span>';
                        }
                        html += '</li>';
                    });
                    html += '</ul>';
                }

                html += '</div></div></div>';
            });
            html += '</div>';

            if (spinner) spinner.classList.add('d-none');
            if (body) body.innerHTML = html || '<p class="text-muted small mb-0">No insights returned.</p>';
            if (status) status.textContent = 'Generated just now';
            _analyticsLastSections = result.sections;

            var auditBtn = document.getElementById('analyticsAuditBtn');
            if (auditBtn) auditBtn.classList.remove('d-none');

            _pushInterpretToChat(result.sections);

        } catch (e) {
            if (spinner) spinner.classList.add('d-none');
            if (body) body.innerHTML = '<div class="alert alert-danger small mb-0"><i class="bi bi-exclamation-triangle me-1"></i>' + _esc(e.message) + '</div>';
        } finally {
            if (btn) btn.disabled = false;
        }
    };

    function _pushInterpretToChat(sections) {
        if (!sections || !sections.length) return;

        var selEl = document.getElementById('analyticsTypeSelect');
        var entityType = selEl && selEl.value
            ? selEl.options[selEl.selectedIndex].text.trim()
            : 'all entity types';

        var markdown = '**Graph Analytics Interpretation — ' + entityType + '**\n\n'
            + _sectionsToMarkdown(sections);

        if (typeof window.appendChatAssistantMessage === 'function') {
            window.appendChatAssistantMessage(markdown);
        } else if (typeof window.chatSendMessage === 'function') {
            var chatLink = document.querySelector('[data-section="chat"]');
            if (chatLink) chatLink.click();
            setTimeout(function () {
                window.chatSendMessage(
                    'Here are the AI-generated insights from the Graph Analytics page:\n\n' + markdown
                );
            }, 500);
        }
    }

    window.analyticsAddToAuditTrail = async function () {
        if (!_analyticsData) return;

        var btn = document.getElementById('analyticsAuditBtn');
        var sections = _analyticsLastSections;
        if (!sections || !sections.length) {
            if (typeof showNotification === 'function')
                showNotification('Run Interpret first to generate insights.', 'warning');
            return;
        }

        var selEl = document.getElementById('analyticsTypeSelect');
        var entityType = selEl && selEl.value
            ? selEl.options[selEl.selectedIndex].text.trim()
            : 'all entity types';

        var stats = _analyticsData.stats || {};
        var header = [
            '**[Graph Analytics — AI Insights] ' + entityType + '**',
            '_' + stats.node_count + ' nodes · ' + stats.edge_count + ' edges · '
                + stats.connected_components + ' component(s)_',
            ''
        ].join('\n');
        var markdown = header + '\n' + _sectionsToMarkdown(sections);

        if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Saving…'; }

        try {
            // Resolve domain context directly — no dependency on the panel's
            // internal ctx variable which may not be set yet.
            var vsResp = await fetch('/domain/version-status', { credentials: 'same-origin' });
            var vs = await vsResp.json();
            if (!vs.domain_folder || !vs.has_registry) {
                throw new Error('Save this domain to the registry first.');
            }

            // POST the comment directly to the API
            var postResp = await fetch(
                '/comments/' + encodeURIComponent(vs.domain_folder) + '/' + encodeURIComponent(vs.version),
                {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ body: markdown, parent_id: null }),
                }
            );
            var postData = await postResp.json();
            if (!postResp.ok || !postData.success) {
                throw new Error(postData.message || 'Failed to post comment');
            }

            if (typeof showNotification === 'function')
                showNotification('Insights saved to audit trail.', 'success');
            if (btn) { btn.innerHTML = '<i class="bi bi-check2 me-1"></i>Saved'; }
            setTimeout(function () {
                if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-journal-plus me-1"></i>Add to audit trail'; }
            }, 3000);

            // Open the discussion panel so the user can see the new comment
            if (typeof openTwinDiscussion === 'function') openTwinDiscussion();

        } catch (e) {
            if (typeof showNotification === 'function')
                showNotification('Could not save to audit trail: ' + e.message, 'danger');
            if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-journal-plus me-1"></i>Add to audit trail'; }
        }
    };

    function _esc(str) {
        return String(str || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    // Minimal markdown → HTML: **bold**, *italic*, newlines → <br>
    function _md(str) {
        return _esc(str)
            .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
            .replace(/\*(.+?)\*/g, '<em>$1</em>')
            .replace(/\n/g, '<br>');
    }

    // Extract the local fragment from a URI (after the last # or /)
    function _localName(uri) {
        return (uri || '').split(/[/#]/).pop() || uri;
    }

    // "Label (localId)" display name for a node URI
    function _displayName(uri) {
        var nodeLabels = _analyticsData ? (_analyticsData.node_labels || {}) : {};
        var localId = _localName(uri);
        var lbl = nodeLabels[uri];
        return lbl ? lbl + ' (' + localId + ')' : localId;
    }

    // Serialize a sections array (from the interpreter) into a markdown string
    function _sectionsToMarkdown(sections) {
        var lines = [];
        sections.forEach(function (sec) {
            lines.push('### ' + sec.title);
            if (sec.body) {
                lines.push(sec.body);
            } else if (Array.isArray(sec.items)) {
                sec.items.forEach(function (item) {
                    if (typeof item === 'string') {
                        lines.push('- ' + item);
                    } else if (item.label) {
                        lines.push('- **' + item.label + '**' + (item.reason ? ' — ' + item.reason : ''));
                    }
                });
            }
            lines.push('');
        });
        return lines.join('\n').trim();
    }

    function _findUriByLabel(label) {
        if (!_analyticsData) return null;
        var nodeLabels = _analyticsData.node_labels || {};
        for (var uri in nodeLabels) {
            if (nodeLabels[uri] === label || _localName(uri) === label) return uri;
        }
        return null;
    }

})();

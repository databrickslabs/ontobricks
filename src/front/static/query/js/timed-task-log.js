/**
 * Shared timed task-log renderer for build workflows.
 */
(function () {
    function _escape(value) {
        if (typeof window.escapeHtml === "function") {
            return window.escapeHtml(value);
        }
        const el = document.createElement("span");
        el.textContent = value == null ? "" : String(value);
        return el.innerHTML;
    }

    function _formatTaskDuration(startISO, endISO) {
        if (typeof formatDuration === "function") {
            return formatDuration(startISO, endISO);
        }
        return "";
    }

    function _computeTaskDuration(task) {
        if (typeof computeTaskDuration === "function") {
            return computeTaskDuration(task);
        }
        return _formatTaskDuration(task && task.started_at, task && task.completed_at);
    }

    function _stepConfig(status) {
        const map = {
            pending: { icon: "bi-circle", colorClass: "text-muted" },
            running: { icon: "bi-arrow-repeat spin-animation", colorClass: "text-primary" },
            completed: { icon: "bi-check-circle-fill", colorClass: "text-success" },
            skipped: { icon: "bi-dash-circle", colorClass: "text-muted" },
            failed: { icon: "bi-x-circle-fill", colorClass: "text-danger" },
        };
        return map[status] || map.pending;
    }

    function create(config) {
        const settings = Object.assign(
            {
                cardId: "",
                listId: "",
                totalId: "",
                badgeId: "",
                exportButtonId: "",
                title: "OntoBricks — Build Log",
                filenamePrefix: "build",
            },
            config || {}
        );

        let tickerStarted = false;
        let lastTask = null;

        function _el(id) {
            return id ? document.getElementById(id) : null;
        }

        function _renderStepTime(step, task) {
            if (step.status === "completed" || step.status === "failed" || step.status === "skipped") {
                const start = step.started_at;
                const end = step.completed_at || step.started_at;
                if (!start) return '<span class="text-muted">—</span>';
                return '<span class="text-muted small">' + _escape(_formatTaskDuration(start, end) || "—") + "</span>";
            }
            if (step.status === "running" && step.started_at) {
                return (
                    '<span class="text-primary small" data-step-elapsed="' +
                    _escape(step.started_at) +
                    '">' +
                    _escape(_formatTaskDuration(step.started_at, null) || "0s") +
                    "</span>"
                );
            }
            return '<span class="text-muted small">—</span>';
        }

        function _renderStepRow(step, idx, runningMessage, task) {
            const cfg = _stepConfig(step.status);
            const desc = _escape(step.description || step.name || "Step " + (idx + 1));

            let detailHtml = "";
            if (step.status === "running" && runningMessage) {
                detailHtml = '<div class="sync-step-detail">' + _escape(runningMessage) + "</div>";
            } else if (step.status === "skipped") {
                detailHtml = '<div class="sync-step-detail text-muted">Not needed for this build</div>';
            } else if (step.status === "failed" && runningMessage) {
                detailHtml = '<div class="sync-step-detail text-danger">' + _escape(runningMessage) + "</div>";
            }

            return (
                '<div class="sync-build-row sync-step-' +
                step.status +
                '">' +
                '<span class="sync-step-icon"><i class="bi ' +
                cfg.icon +
                " " +
                cfg.colorClass +
                '"></i></span>' +
                '<div class="sync-step-body">' +
                '<div class="sync-step-title">' +
                desc +
                "</div>" +
                detailHtml +
                "</div>" +
                '<div class="sync-step-time">' +
                _renderStepTime(step, task) +
                "</div>" +
                "</div>"
            );
        }

        function _ensureTicker() {
            if (tickerStarted) return;
            tickerStarted = true;
            setInterval(function () {
                const root = _el(settings.listId);
                if (!root) return;
                root.querySelectorAll("[data-step-elapsed]").forEach(function (item) {
                    const startISO = item.getAttribute("data-step-elapsed");
                    const txt = _formatTaskDuration(startISO, null);
                    if (txt) item.textContent = txt;
                });
            }, 1000);
        }

        function _formatAsText(task) {
            const lines = [];
            const sep = "─".repeat(60);

            lines.push(sep);
            lines.push(settings.title || "OntoBricks — Build Log");
            lines.push(sep);
            lines.push("Task ID         : " + (task.id || ""));
            lines.push("Name            : " + (task.name || ""));
            lines.push("Status          : " + (task.status || ""));
            if (task.created_at) lines.push("Created at      : " + task.created_at);
            if (task.started_at) lines.push("Started at      : " + task.started_at);
            if (task.completed_at) lines.push("Completed at    : " + task.completed_at);
            const totalDur = _computeTaskDuration(task);
            if (totalDur) lines.push("Total duration  : " + totalDur);
            if (task.progress != null) lines.push("Progress        : " + task.progress + "%");
            if (task.message) lines.push("Last message    : " + task.message);
            if (task.error) lines.push("Error           : " + task.error);
            lines.push("");

            const steps = Array.isArray(task.steps) ? task.steps : [];
            if (steps.length > 0) {
                lines.push("Steps");
                lines.push(sep);
                steps.forEach(function (step, idx) {
                    const num = String(idx + 1).padStart(2, " ");
                    const status = (step.status || "pending").toUpperCase().padEnd(9, " ");
                    const desc = step.description || step.name || "Step " + (idx + 1);
                    lines.push("[" + num + "] " + status + " " + desc);
                    if (step.started_at) lines.push("     started   : " + step.started_at);
                    if (step.completed_at) lines.push("     completed : " + step.completed_at);
                    if (step.started_at) {
                        const duration = _formatTaskDuration(step.started_at, step.completed_at || null);
                        if (duration) {
                            const label = step.completed_at ? "     duration  : " : "     elapsed   : ";
                            lines.push(label + duration);
                        }
                    }
                    if (step.status === "running" && task.message) {
                        lines.push("     detail    : " + task.message);
                    }
                    lines.push("");
                });
            }

            if (task.result && typeof task.result === "object") {
                lines.push("Result");
                lines.push(sep);
                if (task.result.triple_count != null) lines.push("Triples         : " + task.result.triple_count);
                if (task.result.diff && typeof task.result.diff === "object") {
                    lines.push("Diff            : +" + (task.result.diff.added || 0) + " / -" + (task.result.diff.removed || 0));
                }
                if (task.result.view_table) lines.push("View table      : " + task.result.view_table);
                if (task.result.graph_name) lines.push("Graph name      : " + task.result.graph_name);
                if (task.result.duration_seconds != null) lines.push("Duration (s)    : " + task.result.duration_seconds);
                lines.push("");
            }

            lines.push(sep);
            lines.push("Exported at     : " + new Date().toISOString());
            lines.push(sep);
            return lines.join("\n");
        }

        function show() {
            const card = _el(settings.cardId);
            const list = _el(settings.listId);
            const total = _el(settings.totalId);
            const badge = _el(settings.badgeId);
            const exportBtn = _el(settings.exportButtonId);
            if (!card) return;

            card.classList.remove("d-none");
            if (list) {
                list.innerHTML =
                    '<div class="text-muted small py-2"><i class="bi bi-hourglass-split me-1"></i>Waiting for the build to start…</div>';
            }
            if (total) total.textContent = "";
            if (badge) {
                badge.textContent = "running";
                badge.className = "badge bg-primary ms-1";
            }
            lastTask = null;
            if (exportBtn) exportBtn.disabled = true;
            _ensureTicker();
        }

        function hide() {
            const card = _el(settings.cardId);
            if (card) card.classList.add("d-none");
        }

        function render(task) {
            const list = _el(settings.listId);
            const total = _el(settings.totalId);
            const badge = _el(settings.badgeId);
            const exportBtn = _el(settings.exportButtonId);
            if (!list || !task) return;

            lastTask = task;
            if (exportBtn) exportBtn.disabled = false;

            const steps = Array.isArray(task.steps) ? task.steps : [];
            if (steps.length === 0) {
                list.innerHTML = '<div class="text-muted small py-2">No build steps reported.</div>';
                return;
            }

            const runningMessage = task.message || "";
            list.innerHTML = steps
                .map(function (step, idx) {
                    return _renderStepRow(step, idx, runningMessage, task);
                })
                .join("");

            if (total) {
                const dur = _computeTaskDuration(task);
                total.textContent = dur ? "Total: " + dur : "";
            }
            if (badge) {
                if (task.status === "completed") {
                    badge.textContent = "done";
                    badge.className = "badge bg-success ms-1";
                } else if (task.status === "failed") {
                    badge.textContent = "failed";
                    badge.className = "badge bg-danger ms-1";
                } else if (task.status === "cancelled") {
                    badge.textContent = "cancelled";
                    badge.className = "badge bg-warning text-dark ms-1";
                } else {
                    badge.textContent = "running";
                    badge.className = "badge bg-primary ms-1";
                }
            }
        }

        function exportLog() {
            if (!lastTask) {
                if (typeof showNotification === "function") {
                    showNotification("No build log to export yet", "warning");
                }
                return;
            }

            const text = _formatAsText(lastTask);
            const stamp = (lastTask.started_at || lastTask.created_at || new Date().toISOString())
                .replace(/[:.]/g, "-")
                .replace("T", "_")
                .slice(0, 19);
            const prefix = settings.filenamePrefix || "build";
            const filename = prefix + "_" + stamp + ".log";

            const blob = new Blob([text], { type: "text/plain;charset=utf-8" });
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            setTimeout(function () {
                URL.revokeObjectURL(url);
            }, 1000);
        }

        return {
            show: show,
            hide: hide,
            render: render,
            export: exportLog,
        };
    }

    window.TimedTaskLog = Object.freeze({
        create: function (config) {
            return create(config);
        },
    });
})();

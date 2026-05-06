/**
 * Task Tracker - Global async task monitoring
 * 
 * Provides UI for tracking long-running async tasks.
 */

// State
let trackedTasks = [];
let pollInterval = null;
const POLL_INTERVAL_ACTIVE = 3000;  // 3s when tasks are running
const POLL_INTERVAL_IDLE = 30000;   // 30s when idle

// Task type to URL mapping
const TASK_TYPE_URLS = {
    'ontology_generation': '/ontology#wizard',
    'auto_assign': '/mapping#autoassign',
    'metadata_load': '/domain#metadata',
    'metadata_update': '/domain#metadata',
    'triplestore_sync': '/dtwin#sync',
    'quality_checks': '/dtwin#quality'
};

// =====================================================
// INITIALIZATION
// =====================================================

/**
 * Initialize the task tracker
 */
function initTaskTracker() {
    console.log('[TaskTracker] Initializing...');
    
    // Initial fetch
    fetchTasks();
    
    // Start polling
    startPolling();

    // Tick the visible elapsed-time labels every second so users see a
    // smoothly advancing timer rather than the 3s polling cadence.
    setInterval(tickRunningDurations, 1000);

    // Setup event listeners
    setupTaskTrackerEvents();
}

/**
 * Setup event listeners
 */
function setupTaskTrackerEvents() {
    // Close dropdown when clicking outside
    document.addEventListener('click', (e) => {
        const dropdown = document.getElementById('taskTrackerDropdown');
        const toggle = document.getElementById('taskTrackerToggle');
        if (dropdown && toggle && !dropdown.contains(e.target) && !toggle.contains(e.target)) {
            dropdown.classList.remove('show');
        }
    });
}

// =====================================================
// API CALLS
// =====================================================

/**
 * Fetch all tasks from the server.
 *
 * The panel only displays active (pending/running) tasks.  Terminal tasks
 * (completed / failed / cancelled) are converted to notifications the very
 * first time we observe the transition, then dropped from the in-memory
 * active list.
 */
async function fetchTasks() {
    try {
        const response = await fetch('/tasks/', { credentials: 'same-origin' });
        const data = await response.json();

        if (!data.success) return;

        const incoming = data.tasks || [];

        // Detect active → terminal transitions by comparing the previous
        // active snapshot with the fresh payload.  A task that we were
        // tracking as active and is now terminal triggers a notification.
        const previousById = new Map(trackedTasks.map(t => [t.id, t]));
        incoming.forEach(task => {
            const prev = previousById.get(task.id);
            const wasActive = prev && (prev.status === 'pending' || prev.status === 'running');
            const isTerminal = task.status === 'completed'
                || task.status === 'failed'
                || task.status === 'cancelled';
            if (wasActive && isTerminal) {
                notifyTaskTransition(task);
            }
        });

        // Keep only active tasks in local state; the dropdown renders from this.
        trackedTasks = incoming.filter(
            t => t.status === 'pending' || t.status === 'running'
        );
        updateTaskTrackerUI();

        // Adjust polling interval based on active tasks reported by the server.
        adjustPollingInterval(data.active_count || 0);
    } catch (error) {
        console.error('[TaskTracker] Error fetching tasks:', error);
    }
}

/**
 * Push a notification describing a task's final state.
 * Terminal status drives both the bell's type and icon.
 */
function notifyTaskTransition(task) {
    const name = escapeHtml(task.name || 'Task');
    const duration = computeTaskDuration(task);
    const durSuffix = duration ? ` <span class="text-muted small">(in ${duration})</span>` : '';
    let type = 'info';
    let body;
    if (task.status === 'completed') {
        type = 'success';
        body = `Task <strong>${name}</strong> completed${durSuffix}`;
    } else if (task.status === 'failed') {
        type = 'error';
        const err = task.error || task.message || 'Unknown error';
        body = `Task <strong>${name}</strong> failed${durSuffix}: ${escapeHtml(err)}`;
    } else if (task.status === 'cancelled') {
        type = 'warning';
        body = `Task <strong>${name}</strong> cancelled${durSuffix}`;
    } else {
        return;
    }

    // If a known page exists for this task type, make the notification
    // clickable so the user can jump straight to the result.
    const url = TASK_TYPE_URLS[task.task_type];
    if (url) {
        body += ` <a href="${url}" class="ms-1">Open</a>`;
    }

    if (typeof NotificationCenter !== 'undefined' && NotificationCenter.add) {
        NotificationCenter.add(body, type);
    } else if (typeof showNotification === 'function') {
        showNotification(body, type);
    }
}

/**
 * Cancel a task
 */
async function cancelTask(taskId) {
    try {
        const response = await fetch(`/tasks/${taskId}/cancel`, {
            method: 'POST',
            credentials: 'same-origin'
        });
        const data = await response.json();
        
        if (data.success) {
            showNotification('Task cancelled', 'info');
            fetchTasks();
        } else {
            showNotification(data.message || 'Failed to cancel task', 'warning');
        }
    } catch (error) {
        console.error('[TaskTracker] Error cancelling task:', error);
        showNotification('Error cancelling task', 'error');
    }
}

// =====================================================
// POLLING
// =====================================================

/**
 * Start polling for task updates
 */
function startPolling() {
    if (pollInterval) {
        clearInterval(pollInterval);
    }
    pollInterval = setInterval(fetchTasks, POLL_INTERVAL_IDLE);
}

/**
 * Adjust polling interval based on active task count
 */
function adjustPollingInterval(activeCount) {
    const newInterval = activeCount > 0 ? POLL_INTERVAL_ACTIVE : POLL_INTERVAL_IDLE;
    
    if (pollInterval) {
        clearInterval(pollInterval);
    }
    pollInterval = setInterval(fetchTasks, newInterval);
}

/**
 * Force immediate refresh
 */
function refreshTasks() {
    fetchTasks();
}

// =====================================================
// UI UPDATES
// =====================================================

/**
 * Update the task tracker UI
 */
function updateTaskTrackerUI() {
    updateTaskBadge();
    updateTaskDropdown();
}

/**
 * Update the badge showing active task count
 */
function updateTaskBadge() {
    const badge = document.getElementById('taskTrackerBadge');
    if (!badge) return;
    
    const activeCount = trackedTasks.filter(t => 
        t.status === 'pending' || t.status === 'running'
    ).length;
    
    if (activeCount > 0) {
        badge.textContent = activeCount;
        badge.style.display = 'inline-block';
        
        // Add pulse animation for running tasks
        const hasRunning = trackedTasks.some(t => t.status === 'running');
        badge.classList.toggle('pulse-animation', hasRunning);
    } else {
        badge.style.display = 'none';
    }
}

/**
 * Update the dropdown content.
 *
 * Only active (pending / running) tasks are rendered.  Finished tasks
 * live in the Notification Center (see ``notifyTaskTransition``).
 */
function updateTaskDropdown() {
    const container = document.getElementById('taskTrackerList');
    if (!container) return;

    if (trackedTasks.length === 0) {
        container.innerHTML = `
            <div class="text-center text-muted py-4">
                <i class="bi bi-inbox fs-3 d-block mb-2"></i>
                <small>No active tasks</small>
            </div>
        `;
        return;
    }

    let html = '<div class="task-section">';
    html += '<div class="px-3 py-1 bg-light border-bottom"><small class="text-muted fw-semibold">Active</small></div>';
    trackedTasks.forEach(task => {
        html += renderTaskItem(task);
    });
    html += '</div>';

    container.innerHTML = html;
}

/**
 * Render a single active task item (pending or running).
 *
 * Terminal tasks are never rendered — they are surfaced as notifications
 * by ``notifyTaskTransition``.
 */
function renderTaskItem(task) {
    const statusConfig = getTaskStatusConfig(task.status);

    let progressHtml = '';
    if (task.status === 'running') {
        progressHtml = `
            <div class="progress mt-2" style="height: 4px;">
                <div class="progress-bar progress-bar-striped progress-bar-animated"
                     style="width: ${task.progress}%"></div>
            </div>
        `;
        if (task.steps && task.steps.length > 0 && task.current_step < task.steps.length) {
            const currentStep = task.steps[task.current_step];
            progressHtml += `<small class="text-muted d-block mt-1">${currentStep.description}</small>`;
        }
    }

    const actionsHtml = task.status === 'running'
        ? `<button class="btn btn-link btn-sm p-0 text-danger" onclick="event.stopPropagation(); cancelTask('${task.id}')" title="Cancel">
            <i class="bi bi-x-circle"></i>
        </button>`
        : '';

    // Right-column label: live elapsed for running tasks, queued-ago for pending.
    // ``data-task-elapsed`` is targeted by ``tickRunningDurations`` so the timer
    // advances between server polls without re-fetching.
    let rightLabel;
    if (task.status === 'running') {
        const elapsedSrc = task.started_at || task.created_at;
        rightLabel = `<small class="text-muted" data-task-elapsed="${elapsedSrc || ''}" title="Running for">${escapeHtml(computeTaskDuration(task) || '0s')}</small>`;
    } else {
        rightLabel = `<small class="text-muted">${getTimeAgo(task.created_at)}</small>`;
    }

    return `
        <div class="task-item px-3 py-2 border-bottom" data-task-id="${task.id}">
            <div class="d-flex justify-content-between align-items-start">
                <div class="flex-grow-1">
                    <div class="d-flex align-items-center gap-2">
                        <i class="bi ${statusConfig.icon} ${statusConfig.colorClass}"></i>
                        <span class="fw-medium">${escapeHtml(task.name)}</span>
                    </div>
                    <small class="text-muted">${escapeHtml(task.message || statusConfig.label)}</small>
                    ${progressHtml}
                </div>
                <div class="d-flex align-items-center gap-2">
                    ${rightLabel}
                    ${actionsHtml}
                </div>
            </div>
        </div>
    `;
}

/**
 * Re-render the elapsed time on every running row.  Runs on a 1s timer so
 * the duration ticks visibly between the slower server polls (3s active /
 * 30s idle).  Cheap because it only touches the small text node.
 */
function tickRunningDurations() {
    document.querySelectorAll('[data-task-elapsed]').forEach(el => {
        const startISO = el.getAttribute('data-task-elapsed');
        const text = formatDuration(startISO, null);
        if (text) el.textContent = text;
    });
}

/**
 * Get status configuration (icon, color, label)
 */
function getTaskStatusConfig(status) {
    const configs = {
        pending: { icon: 'bi-clock', colorClass: 'text-secondary', label: 'Pending' },
        running: { icon: 'bi-arrow-repeat', colorClass: 'text-primary spin-animation', label: 'Running' },
        completed: { icon: 'bi-check-circle-fill', colorClass: 'text-success', label: 'Completed' },
        failed: { icon: 'bi-x-circle-fill', colorClass: 'text-danger', label: 'Failed' },
        cancelled: { icon: 'bi-slash-circle', colorClass: 'text-warning', label: 'Cancelled' }
    };
    return configs[status] || configs.pending;
}

/**
 * Toggle dropdown visibility
 */
function toggleTaskDropdown(event) {
    if (event) {
        event.preventDefault();
        event.stopPropagation();
    }
    
    const dropdown = document.getElementById('taskTrackerDropdown');
    const toggle = document.getElementById('taskTrackerToggle');
    
    if (dropdown) {
        const isOpen = dropdown.classList.contains('show');
        
        // Close all other dropdowns first
        document.querySelectorAll('.dropdown-menu.show').forEach(menu => {
            if (menu !== dropdown) {
                menu.classList.remove('show');
            }
        });
        
        if (isOpen) {
            dropdown.classList.remove('show');
        } else {
            dropdown.classList.add('show');
            
            // Position the dropdown properly - align to right edge
            if (toggle) {
                const toggleRect = toggle.getBoundingClientRect();
                dropdown.style.position = 'fixed';
                dropdown.style.top = (toggleRect.bottom + 6) + 'px';
                dropdown.style.right = '10px';  // 10px from right edge
                dropdown.style.left = 'auto';
            }
            
            fetchTasks();  // Refresh when opening
        }
    }
}

// =====================================================
// TASK CREATION (for use by other modules)
// =====================================================

/**
 * Create a new task via API and return the task object
 * This is called by other modules to start async operations
 */
async function createTask(name, taskType, steps = []) {
    // Tasks are created by the backend, this just triggers a refresh
    // The actual task creation happens in the backend handler
    await fetchTasks();
}

/**
 * Subscribe to task completion
 * Returns a promise that resolves when the task completes
 */
function waitForTask(taskId, onProgress = null) {
    return new Promise((resolve, reject) => {
        const checkTask = async () => {
            try {
                const response = await fetch(`/tasks/${taskId}`, { credentials: 'same-origin' });
                const data = await response.json();
                
                if (!data.success) {
                    reject(new Error('Task not found'));
                    return;
                }
                
                const task = data.task;
                
                if (onProgress) {
                    onProgress(task);
                }
                
                if (task.status === 'completed') {
                    resolve(task);
                } else if (task.status === 'failed') {
                    reject(new Error(task.error || 'Task failed'));
                } else if (task.status === 'cancelled') {
                    reject(new Error('Task was cancelled'));
                } else {
                    // Still running, check again
                    setTimeout(checkTask, 1000);
                }
            } catch (error) {
                reject(error);
            }
        };
        
        checkTask();
    });
}

// =====================================================
// UTILITIES
// =====================================================

/**
 * Format a duration between two ISO timestamps as a short, human-readable
 * string (e.g. ``"450ms"``, ``"2.4s"``, ``"1m 23s"``, ``"1h 5m"``).
 *
 * If ``endISO`` is null/undefined, ``now`` is used so callers can show a
 * live timer for running tasks.
 */
function formatDuration(startISO, endISO) {
    if (!startISO) return '';
    const start = new Date(startISO);
    if (Number.isNaN(start.getTime())) return '';
    const end = endISO ? new Date(endISO) : new Date();
    if (Number.isNaN(end.getTime())) return '';
    const ms = Math.max(0, end - start);
    if (ms < 1000) return `${ms}ms`;
    const sec = ms / 1000;
    if (sec < 60) return `${sec.toFixed(1)}s`;
    const minTotal = Math.floor(sec / 60);
    const remSec = Math.round(sec - minTotal * 60);
    if (minTotal < 60) return `${minTotal}m ${remSec}s`;
    const hr = Math.floor(minTotal / 60);
    const remMin = minTotal - hr * 60;
    return `${hr}h ${remMin}m`;
}

/**
 * Pick the right anchors and compute a task's duration string.  Prefer
 * the server-provided ``duration_seconds`` so the value matches whatever
 * the backend logged; fall back to client-side computation when missing.
 */
function computeTaskDuration(task) {
    if (typeof task.duration_seconds === 'number') {
        return formatSeconds(task.duration_seconds);
    }
    const start = task.started_at || task.created_at;
    const end = task.completed_at || null;
    return formatDuration(start, end);
}

/** Internal: same scale as formatDuration but starting from a number. */
function formatSeconds(seconds) {
    if (seconds == null) return '';
    if (seconds < 1) return `${Math.round(seconds * 1000)}ms`;
    if (seconds < 60) return `${seconds.toFixed(1)}s`;
    const minTotal = Math.floor(seconds / 60);
    const remSec = Math.round(seconds - minTotal * 60);
    if (minTotal < 60) return `${minTotal}m ${remSec}s`;
    const hr = Math.floor(minTotal / 60);
    const remMin = minTotal - hr * 60;
    return `${hr}h ${remMin}m`;
}

/**
 * Get relative time string
 */
function getTimeAgo(isoString) {
    if (!isoString) return '';
    
    const date = new Date(isoString);
    const now = new Date();
    const diffMs = now - date;
    const diffSec = Math.floor(diffMs / 1000);
    const diffMin = Math.floor(diffSec / 60);
    const diffHour = Math.floor(diffMin / 60);
    
    if (diffSec < 60) return 'just now';
    if (diffMin < 60) return `${diffMin}m ago`;
    if (diffHour < 24) return `${diffHour}h ago`;
    return date.toLocaleDateString();
}

// escapeHtml is provided globally by utils.js

// =====================================================
// EXPOSE GLOBALLY
// =====================================================

window.initTaskTracker = initTaskTracker;
window.toggleTaskDropdown = toggleTaskDropdown;
window.cancelTask = cancelTask;
window.refreshTasks = refreshTasks;
window.waitForTask = waitForTask;
window.trackedTasks = trackedTasks;
window.formatTaskDuration = formatDuration;

// Auto-initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initTaskTracker);
} else {
    initTaskTracker();
}

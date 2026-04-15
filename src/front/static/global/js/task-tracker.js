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
 * Fetch all tasks from server
 */
async function fetchTasks() {
    try {
        const response = await fetch('/tasks/', { credentials: 'same-origin' });
        const data = await response.json();
        
        if (data.success) {
            trackedTasks = data.tasks || [];
            updateTaskTrackerUI();
            
            // Adjust polling interval based on active tasks
            adjustPollingInterval(data.active_count || 0);
        }
    } catch (error) {
        console.error('[TaskTracker] Error fetching tasks:', error);
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

/**
 * Delete a task from history
 */
async function deleteTask(taskId) {
    try {
        const response = await fetch(`/tasks/${taskId}`, {
            method: 'DELETE',
            credentials: 'same-origin'
        });
        const data = await response.json();
        
        if (data.success) {
            fetchTasks();
        }
    } catch (error) {
        console.error('[TaskTracker] Error deleting task:', error);
    }
}

/**
 * Clear all completed tasks
 */
async function clearCompletedTasks() {
    try {
        const response = await fetch('/tasks/clear-completed', {
            method: 'POST',
            credentials: 'same-origin'
        });
        const data = await response.json();
        
        if (data.success) {
            showNotification(`Cleared ${data.cleared_count} tasks`, 'success');
            fetchTasks();
        }
    } catch (error) {
        console.error('[TaskTracker] Error clearing tasks:', error);
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
 * Update the dropdown content
 */
function updateTaskDropdown() {
    const container = document.getElementById('taskTrackerList');
    if (!container) return;
    
    if (trackedTasks.length === 0) {
        container.innerHTML = `
            <div class="text-center text-muted py-4">
                <i class="bi bi-inbox fs-3 d-block mb-2"></i>
                <small>No tasks</small>
            </div>
        `;
        return;
    }
    
    // Build task list HTML
    let html = '';
    
    // Active tasks first
    const activeTasks = trackedTasks.filter(t => t.status === 'pending' || t.status === 'running');
    const completedTasks = trackedTasks.filter(t => t.status !== 'pending' && t.status !== 'running');
    
    if (activeTasks.length > 0) {
        html += '<div class="task-section mb-2">';
        html += '<div class="px-3 py-1 bg-light border-bottom"><small class="text-muted fw-semibold">Active</small></div>';
        activeTasks.forEach(task => {
            html += renderTaskItem(task);
        });
        html += '</div>';
    }
    
    if (completedTasks.length > 0) {
        html += '<div class="task-section">';
        html += `<div class="px-3 py-1 bg-light border-bottom d-flex justify-content-between align-items-center">
            <small class="text-muted fw-semibold">Recent</small>
            <button class="btn btn-link btn-sm p-0 text-muted" onclick="clearCompletedTasks()">Clear all</button>
        </div>`;
        // Show max 5 completed tasks
        completedTasks.slice(0, 5).forEach(task => {
            html += renderTaskItem(task);
        });
        html += '</div>';
    }
    
    container.innerHTML = html;
}

/**
 * Render a single task item
 */
function renderTaskItem(task) {
    const statusConfig = getTaskStatusConfig(task.status);
    const timeAgo = getTimeAgo(task.created_at);
    
    let progressHtml = '';
    if (task.status === 'running') {
        progressHtml = `
            <div class="progress mt-2" style="height: 4px;">
                <div class="progress-bar progress-bar-striped progress-bar-animated" 
                     style="width: ${task.progress}%"></div>
            </div>
        `;
        
        // Show current step if available
        if (task.steps && task.steps.length > 0 && task.current_step < task.steps.length) {
            const currentStep = task.steps[task.current_step];
            progressHtml += `<small class="text-muted d-block mt-1">${currentStep.description}</small>`;
        }
    }
    
    let actionsHtml = '';
    if (task.status === 'running') {
        actionsHtml = `<button class="btn btn-link btn-sm p-0 text-danger" onclick="event.stopPropagation(); cancelTask('${task.id}')" title="Cancel">
            <i class="bi bi-x-circle"></i>
        </button>`;
    } else if (task.status !== 'pending') {
        actionsHtml = `<button class="btn btn-link btn-sm p-0 text-muted" onclick="event.stopPropagation(); deleteTask('${task.id}')" title="Remove">
            <i class="bi bi-trash"></i>
        </button>`;
    }
    
    // Check if task is clickable (completed/failed tasks with a known URL)
    const taskUrl = TASK_TYPE_URLS[task.task_type];
    const isClickable = taskUrl && (task.status === 'completed' || task.status === 'failed');
    const clickHandler = isClickable ? `onclick="navigateToTask('${task.task_type}')"` : '';
    const cursorStyle = isClickable ? 'cursor: pointer;' : '';
    const hoverClass = isClickable ? 'task-item-clickable' : '';
    
    // Add navigate hint for clickable items
    let navigateHint = '';
    if (isClickable) {
        navigateHint = `<small class="text-primary ms-2"><i class="bi bi-box-arrow-up-right"></i></small>`;
    }
    
    return `
        <div class="task-item px-3 py-2 border-bottom ${hoverClass}" data-task-id="${task.id}" ${clickHandler} style="${cursorStyle}">
            <div class="d-flex justify-content-between align-items-start">
                <div class="flex-grow-1">
                    <div class="d-flex align-items-center gap-2">
                        <i class="bi ${statusConfig.icon} ${statusConfig.colorClass}"></i>
                        <span class="fw-medium">${escapeHtml(task.name)}</span>
                        ${navigateHint}
                    </div>
                    <small class="text-muted">${escapeHtml(task.message || statusConfig.label)}</small>
                    ${progressHtml}
                </div>
                <div class="d-flex align-items-center gap-2">
                    <small class="text-muted">${timeAgo}</small>
                    ${actionsHtml}
                </div>
            </div>
        </div>
    `;
}

/**
 * Navigate to the page associated with a task type
 */
function navigateToTask(taskType) {
    const url = TASK_TYPE_URLS[taskType];
    if (url) {
        // Close the dropdown
        const dropdown = document.getElementById('taskTrackerDropdown');
        if (dropdown) {
            dropdown.classList.remove('show');
        }
        
        // Navigate to the URL
        window.location.href = url;
    }
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
window.deleteTask = deleteTask;
window.clearCompletedTasks = clearCompletedTasks;
window.refreshTasks = refreshTasks;
window.waitForTask = waitForTask;
window.trackedTasks = trackedTasks;
window.navigateToTask = navigateToTask;

// Auto-initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initTaskTracker);
} else {
    initTaskTracker();
}

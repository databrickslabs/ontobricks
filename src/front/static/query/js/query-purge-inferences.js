/**
 * Shared purge action for generated reasoning and cohort graph triples.
 */
const InferencePurgeModule = {
    busy: false,

    async purge(button) {
        if (this.busy) return;
        if (window.OB && typeof window.OB.canRefreshGraph === 'function'
                && !window.OB.canRefreshGraph()) {
            showNotification(
                'Purge is unavailable — builder access and graph refresh permission are required.',
                'warning'
            );
            return;
        }

        this._setBusy(true);
        try {
            const status = await this._loadStatus();
            if (!status.purge_supported) {
                showNotification(
                    'The active graph backend cannot safely purge materialized inferences.',
                    'warning'
                );
                return;
            }
            const graphName = this._escapeHtml(
                status.graph_name || button?.dataset.graphName || 'the active graph'
            );
            const count = Number(status.materialized_inference_count || 0);
            const countLabel = count.toLocaleString();
            const confirmed = await showConfirmDialog({
                title: 'Purge materialized inferences?',
                message: `This will delete ${countLabel} materialized inferences `
                    + `(reasoning and cohorts) from ${graphName}.`,
                detailHtml: 'Mapped source triples and external Delta/UC outputs are preserved.',
                confirmText: 'Purge',
                confirmClass: 'btn-danger',
                icon: 'trash',
            });
            if (!confirmed) return;

            const response = await fetch('/dtwin/reasoning/inferred', {
                method: 'DELETE',
                credentials: 'include',
            });
            const data = await response.json();
            if (!response.ok || !data.success) {
                throw new Error(data.message || 'Purge failed');
            }
            this._clearReasoningResult();
            this._publishCount(0);
            showNotification(
                `Purged ${data.purged_count || 0} materialized triples.`,
                'success'
            );
            if (typeof checkTripleStoreStatus === 'function') {
                await checkTripleStoreStatus(true);
            }
        } catch (error) {
            showNotification(error.message || 'Purge failed.', 'error');
        } finally {
            this._setBusy(false);
        }
    },

    async _loadStatus() {
        const response = await fetch('/dtwin/reasoning/inferred', {
            method: 'GET',
            credentials: 'include',
        });
        const data = await response.json();
        if (!response.ok || !data.success) {
            throw new Error(data.message || 'Could not load inference count');
        }
        return data;
    },

    _publishCount(count) {
        document.querySelectorAll('[data-materialized-inference-count]')
            .forEach((element) => {
                element.textContent = Number(count || 0).toLocaleString();
            });
    },

    _setBusy(busy) {
        this.busy = busy;
        document.querySelectorAll('.js-purge-inferences-btn').forEach((button) => {
            button.disabled = busy;
        });
    },

    _clearReasoningResult() {
        if (typeof ReasoningModule === 'undefined') return;
        ReasoningModule._inferredData = [];
        ReasoningModule._inferredPage = 0;
        ReasoningModule._updateTabBadges(0);
        ReasoningModule._refreshInferredPane();
        document.getElementById('materializePanel')?.classList.add('d-none');
    },

    _escapeHtml(value) {
        const element = document.createElement('div');
        element.textContent = String(value || '');
        return element.innerHTML;
    },

    init() {
        document.querySelectorAll('.js-purge-inferences-btn').forEach((button) => {
            button.addEventListener('click', () => this.purge(button));
        });
    },
};

window.InferencePurgeModule = InferencePurgeModule;
document.addEventListener('DOMContentLoaded', () => InferencePurgeModule.init());

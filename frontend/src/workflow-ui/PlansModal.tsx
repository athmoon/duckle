import { useCallback, useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { ArrowRight, Check, Play, Plus, Trash2, Waypoints, X } from 'lucide-react';
import {
    plansDelete,
    plansList,
    plansRun,
    plansSave,
    type Plan,
    type PlanRun,
    type PlanStep,
} from '../tauri-bridge';

/**
 * Author a plan: several pipelines in the order they have to run.
 *
 * A schedule runs one pipeline. A plan runs several, in steps - everything inside a step
 * goes at once, and the next step waits for it. A step that fails stops the ones after it,
 * so nothing runs against data that was never produced. Written down here instead of being
 * three schedules set a few minutes apart and hoped over.
 *
 * This is deliberately NOT called PlanView: that name is taken by the compiled-SQL viewer,
 * which is a different thing entirely. Its CSS lives under `.plan-*` for the same reason,
 * so everything here is `.plans-*`.
 */

type Props = {
    /** Where plans.json lives. Null while no workspace is open. */
    workspacePath: string | null;
    /** The pipelines in this workspace, offered when building a step. */
    pipelines: { id: string; name: string }[];
    onClose: () => void;
};

/**
 * How a step names a pipeline on disk.
 *
 * The web console writes this spelling, so the desktop writes it too: one file is read by
 * both products, and two spellings of the same field is how they start disagreeing. Both
 * readers accept a bare id as well, but only one form gets written.
 */
function stepFile(pipelineId: string): string {
    return `pipelines/${pipelineId}.json`;
}

/** The readable name of whatever a step names, however it was spelled. */
function stepLabel(step: string): string {
    return step
        .replace(/^pipelines[/\\]/, '')
        .replace(/\.json$/, '');
}

/**
 * What to put on screen when something failed.
 *
 * A plan is refused for reasons the person writing it can act on - "step 2 has no
 * pipelines in it" - so the reason has to survive the trip. Over Tauri it arrives as that
 * sentence and nothing else. Over HTTP the web shim wraps it as
 * `plans_save: HTTP 400 {"error":"step 2 has no pipelines in it"}`, which buries the only
 * part that helps. Unwrapped here rather than in the shared shim, which every other modal
 * depends on unchanged.
 */
function readable(err: unknown): string {
    // `String(new Error(msg))` is "Error: msg", and the prefix tells the reader nothing
    // they cannot see from the fact that it is in a red box.
    const text = String(err).replace(/^Error:\s*/, '');
    const brace = text.indexOf('{');
    if (brace >= 0) {
        try {
            const parsed = JSON.parse(text.slice(brace));
            if (parsed && typeof parsed.error === 'string') return parsed.error;
        } catch {
            // Not JSON after all; the whole string is the best answer available.
        }
    }
    return text;
}

function emptyDraft(): Plan {
    return { id: '', name: '', stopOnFailure: true, steps: [{ name: 'Step 1', pipelines: [] }] };
}

export default function PlansModal({ workspacePath, pipelines, onClose }: Props) {
    const [plans, setPlans] = useState<Plan[]>([]);
    const [loading, setLoading] = useState(true);
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState<string | null>(null);
    // Kept apart from `error` on purpose: "the store will not open" stays true while an
    // edit on top of it succeeds or fails, and the two mean very different things.
    const [loadError, setLoadError] = useState<string | null>(null);
    const [editing, setEditing] = useState<Plan | null>(null);
    /** Whether the id in the editor names a plan that already exists. */
    const [replacing, setReplacing] = useState(false);
    const [lastRun, setLastRun] = useState<PlanRun | null>(null);

    const refresh = useCallback(async () => {
        if (!workspacePath) {
            setLoading(false);
            return;
        }
        try {
            const list = await plansList(workspacePath);
            setPlans(list);
            setLoadError(null);
        } catch (err) {
            // Never "you have no plans" when the truth is "the file would not open".
            setLoadError(readable(err));
        } finally {
            setLoading(false);
        }
    }, [workspacePath]);

    useEffect(() => {
        void refresh();
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape' && !editing) onClose();
        };
        document.addEventListener('keydown', onKey);
        return () => document.removeEventListener('keydown', onKey);
    }, [refresh, onClose, editing]);

    const save = async () => {
        if (!editing || !workspacePath) return;
        setBusy(true);
        setError(null);
        try {
            setPlans(await plansSave(workspacePath, editing));
            setEditing(null);
        } catch (err) {
            setError(readable(err));
        } finally {
            setBusy(false);
        }
    };

    const remove = async (id: string) => {
        if (!workspacePath) return;
        setBusy(true);
        setError(null);
        try {
            setPlans(await plansDelete(workspacePath, id));
        } catch (err) {
            setError(readable(err));
        } finally {
            setBusy(false);
        }
    };

    const run = async (id: string) => {
        if (!workspacePath) return;
        setBusy(true);
        setError(null);
        setLastRun(null);
        try {
            setLastRun(await plansRun(workspacePath, id));
        } catch (err) {
            setError(readable(err));
        } finally {
            setBusy(false);
        }
    };

    const startNew = () => {
        setEditing(emptyDraft());
        setReplacing(false);
        setError(null);
    };

    const startEdit = (plan: Plan) => {
        // Deep copy: the draft is edited in place and must not mutate the saved list
        // behind the user, so Cancel really cancels.
        setEditing(JSON.parse(JSON.stringify(plan)) as Plan);
        setReplacing(true);
        setError(null);
    };

    const handleBackdrop = (e: React.MouseEvent) => {
        if (e.target === e.currentTarget && !editing) onClose();
    };

    return createPortal(
        <div
            className="modal-backdrop"
            onClick={handleBackdrop}
            role="dialog"
            aria-modal="true"
            aria-label="Plans"
        >
            <div className="modal modal-plans">
                <div className="modal-header">
                    <div className="modal-title-row">
                        <Waypoints size={16} className="modal-title-icon" />
                        <div>
                            <div className="modal-title">Plans</div>
                            <div className="modal-subtitle">
                                Several pipelines, in an order you chose
                            </div>
                        </div>
                    </div>
                    <button type="button" className="modal-close" onClick={onClose} aria-label="Close">
                        <X size={16} />
                    </button>
                </div>

                <div className="modal-body modal-plans-body">
                    {editing ? (
                        <PlanForm
                            draft={editing}
                            replacing={replacing}
                            pipelines={pipelines}
                            busy={busy}
                            error={error}
                            onChange={setEditing}
                            onSave={save}
                            onCancel={() => {
                                setEditing(null);
                                setError(null);
                            }}
                        />
                    ) : (
                        <>
                            <p className="plans-intro">
                                A schedule runs one pipeline. A plan runs several, in steps:
                                everything in a step goes at once, and the next step waits for
                                it. A step that fails stops the ones after it, so nothing runs
                                against data that was never produced.
                            </p>

                            {loading ? (
                                <div className="schedule-empty">Loading…</div>
                            ) : loadError ? (
                                <div className="schedule-load-error">
                                    <b>Your plans could not be read.</b>
                                    <div className="schedule-load-error-detail">{loadError}</div>
                                    <div className="schedule-load-error-hint">
                                        They are still in <code>plans.json</code> in this workspace,
                                        and nothing here will overwrite it. Repair that file and
                                        reopen this window.
                                    </div>
                                </div>
                            ) : !workspacePath ? (
                                <div className="schedule-empty">
                                    <b>No workspace is open.</b>
                                    <div>A plan belongs to a workspace, alongside the pipelines it orders.</div>
                                </div>
                            ) : plans.length === 0 ? (
                                <div className="schedule-empty">
                                    <b>No plans yet.</b>
                                    <div>
                                        A plan is worth writing down when two pipelines have to run
                                        in a particular order.
                                    </div>
                                </div>
                            ) : (
                                <div className="plans-list">
                                    {plans.map(plan => (
                                        <PlanCard
                                            key={plan.id}
                                            plan={plan}
                                            busy={busy}
                                            run={lastRun && lastRun.planId === plan.id ? lastRun : null}
                                            onRun={() => run(plan.id)}
                                            onEdit={() => startEdit(plan)}
                                            onDelete={() => remove(plan.id)}
                                        />
                                    ))}
                                </div>
                            )}

                            {error ? <div className="modal-error">{error}</div> : null}

                            <div className="modal-footer">
                                <button
                                    type="button"
                                    className="btn btn-primary"
                                    onClick={startNew}
                                    disabled={busy || !workspacePath || !!loadError}
                                >
                                    <Plus size={13} /> New plan
                                </button>
                            </div>
                        </>
                    )}
                </div>
            </div>
        </div>,
        document.body,
    );
}

function PlanCard({
    plan,
    busy,
    run,
    onRun,
    onEdit,
    onDelete,
}: {
    plan: Plan;
    busy: boolean;
    run: PlanRun | null;
    onRun: () => void;
    onEdit: () => void;
    onDelete: () => void;
}) {
    const stepCount = plan.steps.length;
    return (
        <div className="plans-card">
            <div className="plans-card-head">
                <div className="plans-card-title">
                    <div className="plans-card-name">{plan.name || plan.id}</div>
                    <div className="plans-card-meta">
                        {plan.id} · {stepCount} step{stepCount === 1 ? '' : 's'} ·{' '}
                        {plan.stopOnFailure ? 'stops on failure' : 'carries on past failure'}
                    </div>
                </div>
                <div className="plans-card-actions">
                    <button type="button" className="btn btn-small" onClick={onRun} disabled={busy}>
                        <Play size={12} /> Run now
                    </button>
                    <button type="button" className="btn btn-small" onClick={onEdit} disabled={busy}>
                        Edit
                    </button>
                    <button
                        type="button"
                        className="btn btn-icon btn-icon-danger"
                        onClick={onDelete}
                        disabled={busy}
                        aria-label={`Delete ${plan.name || plan.id}`}
                    >
                        <Trash2 size={12} />
                    </button>
                </div>
            </div>

            {/* The chain, drawn as what it is: what runs together, and what waits. */}
            <div className="plans-chain">
                {plan.steps.map((step, i) => (
                    <div className="plans-chain-item" key={i}>
                        {i > 0 ? <ArrowRight size={13} className="plans-arrow" /> : null}
                        <div className="plans-step-box">
                            <div className="plans-step-name">{step.name || `Step ${i + 1}`}</div>
                            {step.pipelines.map(p => (
                                <div className="plans-step-pipe" key={p}>
                                    {stepLabel(p)}
                                </div>
                            ))}
                        </div>
                    </div>
                ))}
            </div>

            {run ? <PlanRunOutcome run={run} /> : null}
        </div>
    );
}

/**
 * What a run did, per pipeline.
 *
 * Every pipeline is listed, including the ones an earlier failure meant nobody attempted:
 * a plan that reports four pipelines when it has six hides the two nothing looked at.
 */
function PlanRunOutcome({ run }: { run: PlanRun }) {
    const rows = run.steps.flatMap(step =>
        step.pipelines.map(p => ({ step: step.name, ...p })),
    );
    return (
        <div className="plans-outcome">
            <div className={`plans-outcome-head is-${run.status === 'ok' ? 'ok' : 'failed'}`}>
                {run.status === 'ok' ? <Check size={13} /> : null}
                {run.status === 'ok' ? 'Everything ran' : 'The plan failed'}
            </div>
            {rows.map((r, i) => (
                <div className="plans-outcome-row" key={i}>
                    <span className={`plans-outcome-status is-${r.status}`}>{r.status}</span>
                    <span className="plans-outcome-pipe">{stepLabel(r.pipeline)}</span>
                    {r.error ? <span className="plans-outcome-error">{r.error}</span> : null}
                </div>
            ))}
        </div>
    );
}

function PlanForm({
    draft,
    replacing,
    pipelines,
    busy,
    error,
    onChange,
    onSave,
    onCancel,
}: {
    draft: Plan;
    replacing: boolean;
    pipelines: { id: string; name: string }[];
    busy: boolean;
    error: string | null;
    onChange: (p: Plan) => void;
    onSave: () => void;
    onCancel: () => void;
}) {
    const setStep = (i: number, step: PlanStep) => {
        const steps = draft.steps.slice();
        steps[i] = step;
        onChange({ ...draft, steps });
    };

    const addPipeline = (i: number, pipelineId: string) => {
        if (!pipelineId) return;
        const step = draft.steps[i];
        const file = stepFile(pipelineId);
        // The same pipeline twice in one step would run it twice at once, against itself.
        if (step.pipelines.includes(file)) return;
        setStep(i, { ...step, pipelines: [...step.pipelines, file] });
    };

    return (
        <div className="plans-form">
            <div className="modal-field">
                <label className="modal-field-label" htmlFor="plan-id">
                    Id
                </label>
                <input
                    id="plan-id"
                    className="modal-input"
                    value={draft.id}
                    // The id is what a schedule points at, so changing it on an existing plan
                    // would leave that schedule pointing at nothing.
                    disabled={replacing}
                    onChange={e => onChange({ ...draft, id: e.target.value })}
                    placeholder="nightly"
                />
            </div>

            <div className="modal-field">
                <label className="modal-field-label" htmlFor="plan-name">
                    Name
                </label>
                <input
                    id="plan-name"
                    className="modal-input"
                    value={draft.name}
                    onChange={e => onChange({ ...draft, name: e.target.value })}
                    placeholder="Nightly load"
                />
            </div>

            <div className="plans-steps">
                {draft.steps.map((step, i) => (
                    <div className="plans-step-edit" key={i}>
                        <div className="plans-step-edit-head">
                            <input
                                className="modal-input"
                                value={step.name}
                                onChange={e => setStep(i, { ...step, name: e.target.value })}
                                placeholder={`Step ${i + 1}`}
                                aria-label={`Name of step ${i + 1}`}
                            />
                            <button
                                type="button"
                                className="btn btn-icon btn-icon-danger"
                                onClick={() =>
                                    onChange({
                                        ...draft,
                                        steps: draft.steps.filter((_, j) => j !== i),
                                    })
                                }
                                disabled={draft.steps.length === 1}
                                aria-label={`Remove step ${i + 1}`}
                            >
                                <Trash2 size={12} />
                            </button>
                        </div>

                        {step.pipelines.length === 0 ? (
                            <div className="plans-step-hint">
                                Nothing in this step yet. Everything you add here runs at the same
                                time.
                            </div>
                        ) : (
                            step.pipelines.map(p => (
                                <div className="plans-step-row" key={p}>
                                    <span>{stepLabel(p)}</span>
                                    <button
                                        type="button"
                                        className="btn btn-icon btn-icon-danger"
                                        onClick={() =>
                                            setStep(i, {
                                                ...step,
                                                pipelines: step.pipelines.filter(x => x !== p),
                                            })
                                        }
                                        aria-label={`Remove ${stepLabel(p)} from step ${i + 1}`}
                                    >
                                        <Trash2 size={12} />
                                    </button>
                                </div>
                            ))
                        )}

                        <select
                            className="modal-input modal-select"
                            value=""
                            onChange={e => {
                                addPipeline(i, e.target.value);
                                e.target.value = '';
                            }}
                            aria-label={`Add a pipeline to step ${i + 1}`}
                        >
                            <option value="">Add a pipeline…</option>
                            {pipelines.map(p => (
                                <option key={p.id} value={p.id}>
                                    {p.name}
                                </option>
                            ))}
                        </select>
                    </div>
                ))}
            </div>

            <button
                type="button"
                className="btn btn-secondary"
                onClick={() =>
                    onChange({
                        ...draft,
                        steps: [
                            ...draft.steps,
                            { name: `Step ${draft.steps.length + 1}`, pipelines: [] },
                        ],
                    })
                }
            >
                <Plus size={13} /> Add a step
            </button>

            <div className="modal-field">
                <label className="schedule-toggle">
                    <input
                        type="checkbox"
                        checked={draft.stopOnFailure}
                        onChange={e => onChange({ ...draft, stopOnFailure: e.target.checked })}
                    />
                    Stop the plan when a step fails
                </label>
            </div>

            {error ? <div className="modal-error">{error}</div> : null}

            <div className="modal-footer">
                <button type="button" className="btn btn-secondary" onClick={onCancel} disabled={busy}>
                    Cancel
                </button>
                <button type="button" className="btn btn-primary" onClick={onSave} disabled={busy}>
                    {busy ? 'Saving…' : 'Save plan'}
                </button>
            </div>
        </div>
    );
}

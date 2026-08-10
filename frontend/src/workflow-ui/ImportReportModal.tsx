import { useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import { X, AlertTriangle } from 'lucide-react';
import { useTranslation } from 'react-i18next';

// Shown once, straight after a legacy job file is translated. Follows the same
// modal convention as ConfirmModal (createPortal + .modal-backdrop / .modal /
// .modal-header / .modal-body / .modal-footer, Escape to close).
//
// The point of this dialog is to stop a translated job from looking finished
// when it is not. An import can put every node on the canvas and still leave a
// pipeline that cannot run - an unmapped component becomes a placeholder, and a
// job that read its credentials from a repository item does not carry them in
// the file. Those are reported here rather than discovered at the first run.
export type ImportReport = {
    jobName: string;
    nodeCount: number;
    translated: number;
    components: [string, number][];
    warnings: string[];
    /** Set when the file could not be read or parsed at all; nothing was opened. */
    error?: string;
};

type Props = {
    report: ImportReport | null;
    onClose: () => void;
};

export default function ImportReportModal({ report, onClose }: Props) {
    const { t } = useTranslation();
    const closeRef = useRef<HTMLButtonElement>(null);

    useEffect(() => {
        if (!report) return;
        closeRef.current?.focus();
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') {
                e.preventDefault();
                onClose();
            }
        };
        document.addEventListener('keydown', onKey);
        return () => document.removeEventListener('keydown', onKey);
    }, [report, onClose]);

    if (!report) return null;

    const { jobName, nodeCount, translated, components, warnings } = report;
    const placeholders = nodeCount - translated;

    return createPortal(
        <div
            className="modal-backdrop"
            role="dialog"
            aria-modal="true"
            aria-label={t('import.reportTitle', 'Job imported')}
            onClick={e => {
                if (e.target === e.currentTarget) onClose();
            }}
        >
            <div className="modal modal-import-report">
                <div className="modal-header">
                    <div className="modal-title">{t('import.reportTitle', 'Job imported')}</div>
                    <button
                        type="button"
                        className="modal-close"
                        onClick={onClose}
                        aria-label={t('common.close', 'Close')}
                    >
                        <X size={16} />
                    </button>
                </div>

                <div className="modal-body">
                    <div className="import-report-summary">
                        <strong>{jobName}</strong>
                        {report.error ? null : (
                            <span>
                                {t(
                                    'import.reportCounts',
                                    '{{translated}} of {{total}} components translated',
                                    { translated, total: nodeCount },
                                )}
                            </span>
                        )}
                    </div>

                    {report.error ? (
                        <>
                            <div className="import-report-notready">
                                {t('import.failed', 'Nothing was imported')}
                            </div>
                            <pre className="import-report-error">{report.error}</pre>
                        </>
                    ) : (
                        <>
                            {placeholders > 0 || warnings.length > 0 ? (
                                <div className="import-report-notready">
                                    {t(
                                        'import.reportNotReady',
                                        'This pipeline is not ready to run yet. Work through the list below first.',
                                    )}
                                </div>
                            ) : (
                                <div className="import-report-clean">
                                    {t(
                                        'import.reportClean',
                                        'Every component translated and nothing left unresolved.',
                                    )}
                                </div>
                            )}

                            {warnings.length > 0 ? (
                                <div className="import-report-section">
                                    <div className="import-report-label">
                                        {t('import.reportNeedsAttention', 'Needs attention ({{n}})', {
                                            n: warnings.length,
                                        })}
                                    </div>
                                    <ul className="import-report-warnings">
                                        {warnings.map((w, i) => (
                                            <li key={i}>
                                                <AlertTriangle size={13} />
                                                <span>{w}</span>
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            ) : null}

                            {components.length > 0 ? (
                                <div className="import-report-section">
                                    <div className="import-report-label">
                                        {t('import.reportComponentsSeen', 'Components in the job file')}
                                    </div>
                                    <ul className="import-report-components">
                                        {components.map(([name, count]) => (
                                            <li key={name}>
                                                <code>{name}</code>
                                                <span>{count}</span>
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            ) : null}
                        </>
                    )}
                </div>

                <div className="modal-footer">
                    <button
                        ref={closeRef}
                        type="button"
                        className="btn btn-primary"
                        onClick={onClose}
                    >
                        {t('common.close', 'Close')}
                    </button>
                </div>
            </div>
        </div>,
        document.body,
    );
}

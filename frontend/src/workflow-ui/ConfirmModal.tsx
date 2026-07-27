import { useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import { X } from 'lucide-react';

// A small reusable confirmation dialog for destructive actions (#208). Follows
// the same modal convention as NewPipelineModal (createPortal + .modal-backdrop
// / .modal / .modal-header / .modal-body / .modal-footer, Escape to cancel).
// Focus starts on Cancel so a stray Enter does not confirm a delete.
type Props = {
    open: boolean;
    title: string;
    message: string;
    confirmLabel?: string;
    cancelLabel?: string;
    danger?: boolean;
    onCancel: () => void;
    onConfirm: () => void;
};

export default function ConfirmModal({
    open,
    title,
    message,
    confirmLabel = 'Delete',
    cancelLabel = 'Cancel',
    danger = true,
    onCancel,
    onConfirm,
}: Props) {
    const cancelRef = useRef<HTMLButtonElement>(null);

    useEffect(() => {
        if (!open) return;
        cancelRef.current?.focus();
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') {
                e.preventDefault();
                onCancel();
            }
        };
        document.addEventListener('keydown', onKey);
        return () => document.removeEventListener('keydown', onKey);
    }, [open, onCancel]);

    if (!open) return null;

    return createPortal(
        <div
            className="modal-backdrop"
            role="dialog"
            aria-modal="true"
            onClick={e => {
                if (e.target === e.currentTarget) onCancel();
            }}
        >
            <div className="modal modal-confirm">
                <div className="modal-header">
                    <div className="modal-title">{title}</div>
                    <button
                        type="button"
                        className="modal-close"
                        onClick={onCancel}
                        aria-label="Close"
                    >
                        <X size={16} />
                    </button>
                </div>
                <div className="modal-body">
                    <div className="modal-confirm-message">{message}</div>
                </div>
                <div className="modal-footer">
                    <button
                        ref={cancelRef}
                        type="button"
                        className="btn btn-secondary"
                        onClick={onCancel}
                    >
                        {cancelLabel}
                    </button>
                    <button
                        type="button"
                        className={'btn btn-primary' + (danger ? ' is-danger' : '')}
                        onClick={onConfirm}
                    >
                        {confirmLabel}
                    </button>
                </div>
            </div>
        </div>,
        document.body,
    );
}

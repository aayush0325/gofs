import { useState } from "react";

type Props = {
  title: string;
  label: string;
  initial?: string;
  placeholder?: string;
  submitLabel?: string;
  onSubmit: (value: string) => Promise<void> | void;
  onClose: () => void;
};

// InputDialog is the small single-field modal used for create/rename/snapshot.
export function InputDialog({
  title, label, initial = "", placeholder, submitLabel = "Create", onSubmit, onClose,
}: Props) {
  const [value, setValue] = useState(initial);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const submit = async () => {
    if (!value.trim()) return;
    setBusy(true);
    setError("");
    try {
      await onSubmit(value.trim());
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : "operation failed");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="modal-backdrop" onMouseDown={onClose}>
      <div className="modal input-modal" onMouseDown={(event) => event.stopPropagation()}>
        <div className="modal-header">
          <h2>{title}</h2>
          <button className="icon-button" onClick={onClose} title="Close">
            ×
          </button>
        </div>
        <label className="field-label" htmlFor="dialog-input">
          {label}
        </label>
        <input
          id="dialog-input"
          autoFocus
          className="text-input"
          value={value}
          placeholder={placeholder}
          onChange={(event) => setValue(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") void submit();
            if (event.key === "Escape") onClose();
          }}
        />
        {error && <div className="form-error">{error}</div>}
        <div className="modal-actions">
          <button className="button ghost" onClick={onClose}>Cancel</button>
          <button className="button primary" onClick={() => void submit()} disabled={busy || !value.trim()}>
            {busy ? "Working…" : submitLabel}
          </button>
        </div>
      </div>
    </div>
  );
}

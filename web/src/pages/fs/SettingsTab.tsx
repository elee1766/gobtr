import { Show, createSignal, createEffect, batch } from "solid-js";
import type { TrackedFilesystem } from "%/v1/filesystem_pb";
import { filesystemClient } from "@/api/client";
import { Card, Button, Alert, LabeledInput } from "@/components/ui";
import { formatNumber } from "@/lib/utils";

export function SettingsTab(props: {
  fs: TrackedFilesystem;
  onSave: () => void;
}) {
  const [editLabel, setEditLabel] = createSignal("");
  const [editBtrbkDir, setEditBtrbkDir] = createSignal("");
  const [saving, setSaving] = createSignal(false);
  const [saveSuccess, setSaveSuccess] = createSignal(false);
  const [error, setError] = createSignal("");

  // Initialize edit fields from fs prop
  createEffect(() => {
    batch(() => {
      setEditLabel(props.fs.label);
      setEditBtrbkDir(props.fs.btrbkSnapshotDir);
    });
  });

  const hasChanges = () =>
    editLabel() !== props.fs.label ||
    editBtrbkDir() !== props.fs.btrbkSnapshotDir;

  const defaultName = () => {
    const parts = props.fs.path.replace(/\/$/, "").split("/");
    return parts[parts.length - 1] || "";
  };

  const handleSave = async () => {
    batch(() => {
      setSaving(true);
      setError("");
      setSaveSuccess(false);
    });

    try {
      await filesystemClient.updateFilesystem({
        id: props.fs.id,
        label: editLabel(),
        btrbkSnapshotDir: editBtrbkDir().replace(/^\//, ""),
      });
      setSaveSuccess(true);
      props.onSave();
      setTimeout(() => setSaveSuccess(false), 3000);
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Card class="p-3 space-y-3">
      <Show when={error()}>
        <Alert type="error">{error()}</Alert>
      </Show>
      <Show when={saveSuccess()}>
        <Alert type="success">settings saved</Alert>
      </Show>

      <LabeledInput
        label="name"
        value={editLabel()}
        onChange={setEditLabel}
        placeholder={defaultName()}
        hint="display name for this filesystem"
        class="max-w-xs"
      />

      <LabeledInput
        label="btrbk snapshot directory"
        value={editBtrbkDir()}
        onChange={setEditBtrbkDir}
        placeholder=".snapshots"
        mono
        hint="subvolumes under this directory will be treated as btrbk snapshots"
        class="max-w-xs"
      />

      <div>
        <Button
          variant="primary"
          disabled={saving() || !hasChanges()}
          onClick={handleSave}
        >
          {saving() ? "saving..." : "save"}
        </Button>
      </div>

      <div class="border-t border-border-default pt-3 mt-3 text-xs text-text-tertiary space-y-1">
        <div>
          <span>path: </span>
          <span class="font-mono text-text-default">{props.fs.path}</span>
        </div>
        <div>
          <span>id: </span>
          <span class="text-text-default">{formatNumber(props.fs.id)}</span>
        </div>
      </div>
    </Card>
  );
}

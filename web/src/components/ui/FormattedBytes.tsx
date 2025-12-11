import { Show, type Component, createMemo } from "solid-js";
import { formatBytesParts } from "@/lib/utils";
import { uiSettings } from "@/stores/ui";

export const FormattedBytes: Component<{
  bytes: bigint;
  showRaw: boolean;
}> = (props) => {
  const formatted = createMemo(() => {
    const settings = uiSettings();
    return formatBytesParts(props.bytes, {
      maxUnit: settings.maxByteUnit,
      base: settings.byteBase,
    });
  });

  return (
    <Show
      when={props.showRaw}
      fallback={
        <span>
          <span class="select-all">{formatted().value}</span>
          <span class="text-text-muted ml-0.5">{formatted().unit}</span>
        </span>
      }
    >
      <span class="select-all">{props.bytes.toString()}</span>
    </Show>
  );
};

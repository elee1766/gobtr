import { FragMapViz } from "@/components/FragMapViz";

export function VisualizeTab(props: { fsPath: string }) {
  return <FragMapViz fsPath={props.fsPath} />;
}

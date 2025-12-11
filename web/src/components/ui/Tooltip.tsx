import { Show, createSignal, splitProps, type Component, type JSX } from "solid-js";

export interface TooltipProps {
  text: string;
  children: JSX.Element;
}

export const Tooltip: Component<TooltipProps> = (props) => {
  const [show, setShow] = createSignal(false);
  const [pos, setPos] = createSignal({ x: 0, y: 0 });

  const onEnter = (e: MouseEvent) => {
    setPos({ x: e.clientX, y: e.clientY });
    setShow(true);
  };

  return (
    <div
      class="relative"
      onMouseEnter={onEnter}
      onMouseMove={(e) => setPos({ x: e.clientX, y: e.clientY })}
      onMouseLeave={() => setShow(false)}
    >
      {props.children}
      <Show when={show()}>
        <div
          class="fixed z-50 px-1.5 py-0.5 text-xs bg-bg-overlay text-text-inverse rounded pointer-events-none whitespace-nowrap"
          style={{ left: `${pos().x + 8}px`, top: `${pos().y - 24}px` }}
        >
          {props.text}
        </div>
      </Show>
    </div>
  );
};

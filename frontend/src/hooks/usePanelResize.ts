import { useCallback, useState } from "react";

type Direction = "left" | "right";

export interface ResizeProps {
  onMouseDown: (e: React.MouseEvent) => void;
  onTouchStart: (e: React.TouchEvent) => void;
}

export function usePanelResize(
  setter: (width: number) => void,
  min: number,
  max: number,
  direction: Direction,
) {
  const [resizing, setResizing] = useState(false);

  const clamp = useCallback(
    (clientX: number) => {
      const value =
        direction === "right"
          ? globalThis.innerWidth - clientX
          : clientX;
      setter(Math.max(min, Math.min(max, value)));
    },
    [setter, min, max, direction],
  );

  const handleMouseDown = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      setResizing(true);
      document.body.style.cursor = "col-resize";
      document.body.style.userSelect = "none";
      const onMouseMove = (e: MouseEvent) => clamp(e.clientX);
      const onMouseUp = () => {
        setResizing(false);
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
        globalThis.removeEventListener("mousemove", onMouseMove);
        globalThis.removeEventListener("mouseup", onMouseUp);
      };
      globalThis.addEventListener("mousemove", onMouseMove);
      globalThis.addEventListener("mouseup", onMouseUp);
    },
    [clamp],
  );

  const handleTouchStart = useCallback(
    (_e: React.TouchEvent) => {
      setResizing(true);
      document.body.style.userSelect = "none";
      const onTouchMove = (e: TouchEvent) => {
        e.preventDefault();
        clamp(e.touches[0]!.clientX);
      };
      const onTouchEnd = () => {
        setResizing(false);
        document.body.style.userSelect = "";
        globalThis.removeEventListener("touchmove", onTouchMove);
        globalThis.removeEventListener("touchend", onTouchEnd);
      };
      globalThis.addEventListener("touchmove", onTouchMove, { passive: false });
      globalThis.addEventListener("touchend", onTouchEnd);
    },
    [clamp],
  );

  const resizeProps: ResizeProps = { onMouseDown: handleMouseDown, onTouchStart: handleTouchStart };

  return { resizeProps, resizing };
}

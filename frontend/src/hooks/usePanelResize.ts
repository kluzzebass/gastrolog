import { useCallback, useState } from "react";

type Direction = "left" | "right";

export function usePanelResize(
  setter: (width: number) => void,
  min: number,
  max: number,
  direction: Direction,
) {
  const [resizing, setResizing] = useState(false);

  const handleResize = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      setResizing(true);
      document.body.style.cursor = "col-resize";
      document.body.style.userSelect = "none";
      const onMouseMove = (e: MouseEvent) => {
        const value =
          direction === "right"
            ? globalThis.innerWidth - e.clientX
            : e.clientX;
        setter(Math.max(min, Math.min(max, value)));
      };
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
    [setter, min, max, direction],
  );

  return { handleResize, resizing };
}

import { useState, useEffect } from "react";
import { usePanelResize } from "./usePanelResize";
import { useMediaQuery } from "./useMediaQuery";

export function usePanelLayout() {
  const isTablet = useMediaQuery("(max-width: 1023px)");

  const [sidebarWidth, setSidebarWidth] = useState(224);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [detailWidth, setDetailWidth] = useState(320);
  const [detailCollapsed, setDetailCollapsed] = useState(true);
  const [detailPinned, setDetailPinned] = useState(false);

  const { resizeProps: sidebarResizeProps, resizing: sidebarResizing } =
    usePanelResize(setSidebarWidth, 160, 400, "left");
  const { resizeProps: detailResizeProps, resizing: detailResizing } =
    usePanelResize(setDetailWidth, 240, 600, "right");

  const resizing = sidebarResizing || detailResizing;

  // Auto-collapse both sidebars when entering tablet viewport.
  useEffect(() => {
    if (isTablet) {
      setSidebarCollapsed(true);
      setDetailCollapsed(true);
    }
  }, [isTablet]);

  return {
    isTablet,
    sidebarWidth,
    sidebarCollapsed,
    setSidebarCollapsed,
    sidebarResizeProps,
    detailWidth,
    detailCollapsed,
    setDetailCollapsed,
    detailPinned,
    setDetailPinned,
    detailResizeProps,
    resizing,
  };
}

import { useState } from "react";
import { usePanelResize } from "./usePanelResize";

export function usePanelLayout() {
  const [sidebarWidth, setSidebarWidth] = useState(224);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [detailWidth, setDetailWidth] = useState(320);
  const [detailCollapsed, setDetailCollapsed] = useState(true);
  const [detailPinned, setDetailPinned] = useState(false);

  const { handleResize: handleSidebarResize, resizing: sidebarResizing } =
    usePanelResize(setSidebarWidth, 160, 400, "left");
  const { handleResize: handleDetailResize, resizing: detailResizing } =
    usePanelResize(setDetailWidth, 240, 600, "right");

  const resizing = sidebarResizing || detailResizing;

  return {
    sidebarWidth,
    sidebarCollapsed,
    setSidebarCollapsed,
    handleSidebarResize,
    detailWidth,
    detailCollapsed,
    setDetailCollapsed,
    detailPinned,
    setDetailPinned,
    handleDetailResize,
    resizing,
  };
}

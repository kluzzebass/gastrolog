import { createContext, useContext } from "react";
import type { ProtoRecord } from "../utils";
import type { HighlightMode } from "../syntax";

interface DetailPanelContextValue {
  onFieldSelect: (key: string, value: string) => void;
  onChunkSelect: (chunkId: string) => void;
  onVaultSelect: (vaultId: string) => void;
  onPosSelect: (chunkId: string, pos: string) => void;
  contextBefore: ProtoRecord[];
  contextAfter: ProtoRecord[];
  contextLoading: boolean;
  contextReversed: boolean;
  onContextRecordSelect: (rec: ProtoRecord) => void;
  highlightMode: HighlightMode;
}

const DetailPanelContext = createContext<DetailPanelContextValue | null>(null);

export function DetailPanelProvider({
  children,
  value,
}: Readonly<{
  children: React.ReactNode;
  value: DetailPanelContextValue;
}>) {
  return (
    <DetailPanelContext.Provider value={value}>
      {children}
    </DetailPanelContext.Provider>
  );
}

export function useDetailPanel(): DetailPanelContextValue {
  const ctx = useContext(DetailPanelContext);
  if (!ctx) throw new Error("useDetailPanel must be used within DetailPanelProvider");
  return ctx;
}

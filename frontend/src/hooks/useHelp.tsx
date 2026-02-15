import { createContext, useContext } from "react";

interface HelpContextValue {
  openHelp: (topicId?: string) => void;
}

const HelpContext = createContext<HelpContextValue | null>(null);

export function HelpProvider({
  children,
  onOpen,
}: {
  children: React.ReactNode;
  onOpen: (topicId?: string) => void;
}) {
  return (
    <HelpContext.Provider value={{ openHelp: onOpen }}>
      {children}
    </HelpContext.Provider>
  );
}

export function useHelp(): HelpContextValue {
  const ctx = useContext(HelpContext);
  if (!ctx) throw new Error("useHelp must be used within HelpProvider");
  return ctx;
}

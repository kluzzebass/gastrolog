import { createContext, useContext } from "react";

interface HelpContextValue {
  openHelp: (helpRef?: string) => void;
}

const HelpContext = createContext<HelpContextValue | null>(null);

export function HelpProvider({
  children,
  onOpen,
}: Readonly<{
  children: React.ReactNode;
  onOpen: (helpRef?: string) => void;
}>) {
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

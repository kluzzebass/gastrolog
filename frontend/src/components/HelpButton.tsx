import { useHelp } from "../hooks/useHelp";

export function HelpButton({ topicId }: { topicId?: string }) {
  const { openHelp } = useHelp();

  return (
    <button
      onClick={() => openHelp(topicId)}
      aria-label="Help"
      title="Help"
      className="w-5 h-5 flex items-center justify-center rounded-full text-[0.7em] font-mono font-medium text-text-ghost hover:text-copper hover:bg-copper/10 transition-colors cursor-pointer"
    >
      ?
    </button>
  );
}

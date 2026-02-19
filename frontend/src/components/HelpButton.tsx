import { useHelp } from "../hooks/useHelp";

export function HelpButton({ topicId }: Readonly<{ topicId?: string }>) {
  const { openHelp } = useHelp();

  return (
    <button
      onClick={() => openHelp(topicId)}
      aria-label="Help"
      title="Help"
      className="w-5 h-5 flex items-center justify-center rounded-full text-text-ghost hover:text-copper hover:bg-copper/10 transition-colors cursor-pointer"
    >
      <svg
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        className="w-4 h-4"
      >
        <circle cx="12" cy="12" r="10" />
        <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3" />
        <line x1="12" y1="17" x2="12.01" y2="17" />
      </svg>
    </button>
  );
}

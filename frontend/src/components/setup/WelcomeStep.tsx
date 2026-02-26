import { useThemeClass } from "../../hooks/useThemeClass";
import { PrimaryButton } from "../settings/Buttons";

interface WelcomeStepProps {
  dark: boolean;
  onNext: () => void;
}

export function WelcomeStep({ dark, onNext }: Readonly<WelcomeStepProps>) {
  const c = useThemeClass(dark);
  return (
    <div className="flex flex-col items-center text-center gap-6 py-4">
      <img src="/favicon.svg" alt="GastroLog" className="w-16 h-16 opacity-80" />
      <div className="flex flex-col gap-2">
        <h2
          className={`text-2xl font-display font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Welcome to GastroLog
        </h2>
        <p
          className={`text-[0.9em] leading-relaxed max-w-md ${c("text-text-muted", "text-light-text-muted")}`}
        >
          GastroLog is a log management system built around chunk-based storage
          and indexing. Let's set up your first vault and ingester to start
          collecting logs.
        </p>
      </div>
      <PrimaryButton onClick={onNext}>Get Started</PrimaryButton>
    </div>
  );
}

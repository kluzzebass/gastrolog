import type { GetSettingsResponse } from "../../api/gen/gastrolog/v1/system_pb";

interface PasswordRulesProps {
  password: string;
  confirmPassword?: string;
  config: GetSettingsResponse;
  dark: boolean;
}

export function PasswordRules({ password, confirmPassword, config, dark }: Readonly<PasswordRulesProps>) {
  const pp = config.auth?.passwordPolicy;
  const minLength = pp?.minLength || 8;

  const rules: { label: string; met: boolean; error?: boolean }[] = [
    { label: `At least ${minLength} characters`, met: password.length >= minLength },
  ];

  if (pp?.requireMixedCase) {
    rules.push({
      label: "Upper and lowercase letters",
      met: /[a-z]/.test(password) && /[A-Z]/.test(password),
    });
  }

  if (pp?.requireDigit) {
    rules.push({
      label: "At least one number",
      met: /\d/.test(password),
    });
  }

  if (pp?.requireSpecial) {
    rules.push({
      label: "At least one special character",
      met: /[^a-zA-Z0-9]/.test(password),
    });
  }

  if (pp && pp.maxConsecutiveRepeats > 0) {
    const max = pp.maxConsecutiveRepeats;
    const pattern = new RegExp(`(.)\\1{${max},}`);
    rules.push({
      label: `No more than ${max} identical characters in a row`,
      met: !pattern.test(password),
    });
  }

  if (pp?.forbidAnimalNoise) {
    const noises = [
      "moo", "woof", "bark", "meow", "oink", "quack", "baa", "neigh",
      "roar", "hiss", "chirp", "tweet", "cluck", "ribbit", "buzz",
      "howl", "purr", "squeak", "growl", "caw", "gobble",
    ];
    const lower = password.toLowerCase();
    rules.push({
      label: "No animal noises",
      met: !noises.some((n) => lower.includes(n)),
    });
  }

  if (confirmPassword != null) {
    const matches = password === confirmPassword;
    rules.push({
      label: "Passwords match",
      met: confirmPassword.length > 0 && matches,
      error: password.length > 0 && !matches,
    });
  }

  const c = dark ? (d: string) => d : (_: string, l: string) => l;
  const ruleColorCls = (error: boolean | undefined, theme: typeof c) =>
    error ? "text-severity-error" : theme("text-text-ghost", "text-light-text-ghost");

  return (
    <div className="-mt-2 flex flex-col gap-1">
      {rules.map((rule) => (
        <div
          key={rule.label}
          className={`flex items-center gap-1.5 text-[0.78em] ${
            rule.met
              ? "text-severity-info"
              : ruleColorCls(rule.error, c)
          }`}
        >
          <span className="text-[0.9em]">{rule.met ? "\u2713" : "\u2022"}</span>
          {rule.label}
        </div>
      ))}
    </div>
  );
}

import type { GetServerConfigResponse } from "../../api/gen/gastrolog/v1/config_pb";

interface PasswordRulesProps {
  password: string;
  config: GetServerConfigResponse;
  dark: boolean;
}

export function PasswordRules({ password, config, dark }: Readonly<PasswordRulesProps>) {
  const minLength = config.minPasswordLength || 8;

  const rules: { label: string; met: boolean }[] = [
    { label: `At least ${minLength} characters`, met: password.length >= minLength },
  ];

  if (config.requireMixedCase) {
    rules.push({
      label: "Upper and lowercase letters",
      met: /[a-z]/.test(password) && /[A-Z]/.test(password),
    });
  }

  if (config.requireDigit) {
    rules.push({
      label: "At least one number",
      met: /\d/.test(password),
    });
  }

  if (config.requireSpecial) {
    rules.push({
      label: "At least one special character",
      met: /[^a-zA-Z0-9]/.test(password),
    });
  }

  if (config.maxConsecutiveRepeats > 0) {
    const max = config.maxConsecutiveRepeats;
    const pattern = new RegExp(`(.)\\1{${max},}`);
    rules.push({
      label: `No more than ${max} identical characters in a row`,
      met: !pattern.test(password),
    });
  }

  if (config.forbidAnimalNoise) {
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

  const c = dark ? (d: string) => d : (_: string, l: string) => l;

  return (
    <div className="-mt-2 flex flex-col gap-1">
      {rules.map((rule) => (
        <div
          key={rule.label}
          className={`flex items-center gap-1.5 text-[0.78em] ${
            rule.met
              ? "text-severity-info"
              : c("text-text-ghost", "text-light-text-ghost")
          }`}
        >
          <span className="text-[0.9em]">{rule.met ? "\u2713" : "\u2022"}</span>
          {rule.label}
        </div>
      ))}
    </div>
  );
}

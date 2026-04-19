import { useState, useReducer, useEffect } from "react";
import { useNavigate } from "@tanstack/react-router";
import { useThemeSync } from "../../hooks/useThemeSync";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useToast } from "../Toast";
import { systemClient } from "../../api/client";
import { usePutSetupSettings } from "../../api/hooks/useSettings";
import { useGenerateName } from "../../api/hooks/useSystem";
import { useQueryClient } from "@tanstack/react-query";
import { Button } from "../settings/Buttons";
import { WelcomeStep } from "./WelcomeStep";
import { VaultStep, type VaultData } from "./VaultStep";
import {
  RotationPolicyStep,
  RetentionPolicyStep,
  parseDurationToSeconds,
  parseBytesToBigInt,
  type RotationData,
  type RetentionData,
} from "./PoliciesStep";
import { IngesterStep, type IngesterData } from "./IngesterStep";
import { ReviewStep } from "./ReviewStep";
import { extractMessage } from "../../utils/errors";

const STEPS = ["Welcome", "Vault", "Rotation", "Retention", "Ingester", "Review"] as const;

// -- Reducer for wizard step data --

interface WizardDataState {
  vault: VaultData;
  rotation: RotationData;
  retention: RetentionData;
  ingester: IngesterData;
}

const wizardDataInitial: WizardDataState = {
  vault: { name: "", type: "file", dir: "" },
  rotation: { name: "", maxAge: "", maxBytes: "", maxRecords: "", cron: "" },
  retention: { name: "", maxChunks: "", maxAge: "", maxBytes: "" },
  ingester: { name: "", type: "", params: {} },
};

type WizardDataAction =
  | { type: "setVault"; value: VaultData }
  | { type: "setRotation"; value: RotationData }
  | { type: "setRetention"; value: RetentionData }
  | { type: "setIngester"; value: IngesterData };

function wizardDataReducer(state: WizardDataState, action: WizardDataAction): WizardDataState {
  switch (action.type) {
    case "setVault":
      return { ...state, vault: action.value };
    case "setRotation":
      return { ...state, rotation: action.value };
    case "setRetention":
      return { ...state, retention: action.value };
    case "setIngester":
      return { ...state, ingester: action.value };
    default:
      return state;
  }
}

export function SetupWizard() {
  const { dark } = useThemeSync();
  const c = useThemeClass(dark);
  const navigate = useNavigate();
  const { addToast } = useToast();
  const queryClient = useQueryClient();
  const putSetupSettings = usePutSetupSettings();

  const [step, setStep] = useState(0);
  const [creating, setCreating] = useState(false);
  const generateName = useGenerateName();

  // Step data
  const [wizardData, dispatchData] = useReducer(wizardDataReducer, wizardDataInitial);
  const { vault, rotation, retention, ingester } = wizardData;
  const setVault = (v: VaultData) => dispatchData({ type: "setVault", value: v });
  const setRotation = (v: RotationData) => dispatchData({ type: "setRotation", value: v });
  const setRetention = (v: RetentionData) => dispatchData({ type: "setRetention", value: v });
  const setIngester = (v: IngesterData) => dispatchData({ type: "setIngester", value: v });

  // Generate petname placeholders for each entity on mount.
  const [namePlaceholders, setNamePlaceholders] = useState({
    vault: "",
    rotation: "",
    retention: "",
    ingester: "",
  });

  useEffect(() => {
    async function generateNames() {
      const [vn, rn, ren, inn] = await Promise.all([
        generateName.mutateAsync(),
        generateName.mutateAsync(),
        generateName.mutateAsync(),
        generateName.mutateAsync(),
      ]);
      setNamePlaceholders({ vault: vn, rotation: rn, retention: ren, ingester: inn });
    }
    generateNames();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const canProceed = () => {
    switch (step) {
      case 0: return true; // Welcome
      case 1: // Vault
        if (vault.type === "file" && !vault.dir.trim()) return false;
        return true;
      case 2: return true; // Rotation (defaults are fine)
      case 3: return true; // Retention (defaults are fine)
      case 4: return !!ingester.type; // Need a type selected
      case 5: return true; // Review
      default: return false;
    }
  };

  const handleCreate = async () => {
    setCreating(true);

    // Extract conditional expressions before try/catch for React Compiler compatibility
    const hasRotation = rotation.maxAge || rotation.maxBytes || rotation.maxRecords || rotation.cron;
    const rotationMaxBytes = parseBytesToBigInt(rotation.maxBytes);
    const rotationMaxRecords = rotation.maxRecords ? BigInt(rotation.maxRecords) : BigInt(0);
    const rotationName = rotation.name || namePlaceholders.rotation || "default";

    const hasRetention = retention.maxChunks || retention.maxAge || retention.maxBytes;
    const retentionMaxChunks = retention.maxChunks ? BigInt(retention.maxChunks) : BigInt(0);
    const retentionMaxAge = retention.maxAge || "";
    const retentionMaxBytes = parseBytesToBigInt(retention.maxBytes);
    const retentionName = retention.name || namePlaceholders.retention || "default";

    const vaultName = vault.name || namePlaceholders.vault || "default";
    const ingesterName = ingester.name || namePlaceholders.ingester || ingester.type;

    // Pre-compute IDs and derived values outside try/catch so the React
    // Compiler can analyze conditional/logical expressions (it can't
    // optimize value blocks inside try/catch).
    const filterIdBytes = crypto.getRandomValues(new Uint8Array(16));
    const vaultIdBytes = crypto.getRandomValues(new Uint8Array(16));
    const ingesterIdBytes = crypto.getRandomValues(new Uint8Array(16));
    const rotationIdBytes = hasRotation ? crypto.getRandomValues(new Uint8Array(16)) : new Uint8Array(16);
    const retentionIdBytes = hasRetention ? crypto.getRandomValues(new Uint8Array(16)) : new Uint8Array(16);

    // Setup wizard should create a TierConfig with the rotation/retention
    // policies and vault type, with vaultId pointing to this vault.
    // For now, policies are created but not linked to tiers (gastrolog-e0s05).

    // Build policy promises outside try so the compiler can optimize conditionals.
    const policyPromises: Promise<unknown>[] = [
      systemClient.putFilter({
        config: { id: filterIdBytes, name: "catch-all", expression: "*" },
      }),
    ];
    if (hasRotation) {
      policyPromises.push(
        systemClient.putRotationPolicy({
          config: {
            id: rotationIdBytes,
            name: rotationName,
            maxAgeSeconds: parseDurationToSeconds(rotation.maxAge),
            maxBytes: rotationMaxBytes,
            maxRecords: rotationMaxRecords,
            cron: rotation.cron,
          },
        }),
      );
    }
    if (hasRetention) {
      policyPromises.push(
        systemClient.putRetentionPolicy({
          config: {
            id: retentionIdBytes,
            name: retentionName,
            maxChunks: retentionMaxChunks,
            maxAgeSeconds: parseDurationToSeconds(retentionMaxAge),
            maxBytes: retentionMaxBytes,
          },
        }),
      );
    }

    try {
      // 1. Create independent policies in parallel.
      await Promise.all(policyPromises);

      // 2. Create vault (tier assignment will be handled in a follow-up).
      await systemClient.putVault({
        config: {
          id: vaultIdBytes,
          name: vaultName,
          enabled: true,
        },
      });

      // 3. Create route + ingester in parallel (route references filter + vault).
      await Promise.all([
        systemClient.putRoute({
          config: {
            id: crypto.getRandomValues(new Uint8Array(16)),
            name: "default",
            filterId: filterIdBytes,
            destinations: [{ vaultId: vaultIdBytes }],
            distribution: "fanout",
            enabled: true,
          },
        }),
        systemClient.putIngester({
          config: {
            id: ingesterIdBytes,
            name: ingesterName,
            type: ingester.type,
            enabled: true,
            params: ingester.params,
          },
        }),
      ]);

      await putSetupSettings.mutateAsync(true);
      // Optimistically update the cache so SearchView's redirect doesn't
      // fire before the follower's FSM applies the Raft log entry.
      queryClient.setQueryData(["settings"], (old: Record<string, unknown> | undefined) => {
        if (!old) return old;
        return { ...old, setupWizardDismissed: true };
      });
      await queryClient.invalidateQueries({ queryKey: ["system"], refetchType: "all" });
      addToast("Configuration created successfully!", "info");
      setCreating(false);
      navigate({ to: "/search", search: { q: "", help: undefined, settings: undefined, inspector: undefined } });
    } catch (err) {
      addToast(extractMessage(err, "Failed to create configuration"), "error");
      setCreating(false);
    }
  };

  return (
    <div className="flex-1 flex items-center justify-center overflow-auto p-4">
      <div
        className={`w-full max-w-xl mx-auto rounded-lg border shadow-lg overflow-hidden ${c(
          "border-ink-border bg-ink-raised",
          "border-light-border bg-light-surface",
        )}`}
      >
        {/* Step indicator */}
        <div
          className={`flex items-center justify-center gap-2 px-6 py-4 border-b ${c(
            "border-ink-border-subtle",
            "border-light-border-subtle",
          )}`}
        >
          {STEPS.map((label, i) => {
            let stepClass: string;
            if (i === step) {
              stepClass = "bg-copper text-text-on-copper";
            } else if (i < step) {
              stepClass = c(
                "bg-copper/20 text-copper cursor-pointer hover:bg-copper/30",
                "bg-copper/20 text-copper cursor-pointer hover:bg-copper/30",
              );
            } else {
              stepClass = c(
                "bg-ink-surface text-text-muted",
                "bg-light-well text-light-text-muted",
              );
            }
            return (
            <div key={label} className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => i < step && setStep(i)}
                disabled={i >= step}
                className={`w-7 h-7 rounded-full flex items-center justify-center text-[0.75em] font-mono font-medium transition-colors ${stepClass}`}
              >
                {i + 1}
              </button>
              {i < STEPS.length - 1 && (
                <div
                  className={`w-6 h-px ${c(
                    i < step ? "bg-copper/40" : "bg-ink-border-subtle",
                    i < step ? "bg-copper/40" : "bg-light-border-subtle",
                  )}`}
                />
              )}
            </div>
            );
          })}
        </div>

        {/* Step content */}
        <div className="px-6 py-5 max-h-[60vh] overflow-y-auto app-scroll">
          {step === 0 && (
            <WelcomeStep dark={dark} onNext={() => setStep(1)} />
          )}
          {step === 1 && (
            <VaultStep dark={dark} data={vault} onChange={setVault} namePlaceholder={namePlaceholders.vault} />
          )}
          {step === 2 && (
            <RotationPolicyStep
              dark={dark}
              rotation={rotation}
              onRotationChange={setRotation}
              rotationNamePlaceholder={namePlaceholders.rotation}
            />
          )}
          {step === 3 && (
            <RetentionPolicyStep
              dark={dark}
              retention={retention}
              onRetentionChange={setRetention}
              retentionNamePlaceholder={namePlaceholders.retention}
            />
          )}
          {step === 4 && (
            <IngesterStep dark={dark} data={ingester} onChange={setIngester} namePlaceholder={namePlaceholders.ingester} />
          )}
          {step === 5 && (
            <ReviewStep
              dark={dark}
              vault={vault}
              rotation={rotation}
              retention={retention}
              ingester={ingester}
              namePlaceholders={namePlaceholders}
            />
          )}
        </div>

        {/* Navigation */}
        <div
          className={`flex items-center justify-between px-6 py-4 border-t ${c(
            "border-ink-border-subtle",
            "border-light-border-subtle",
          )}`}
        >
          <div className="flex gap-2">
            {step > 0 && (
              <Button variant="ghost"
                onClick={() => setStep((s) => s - 1)}
                dark={dark}
                bordered
              >
                Back
              </Button>
            )}
            <Button variant="ghost"
              onClick={async () => {
                await putSetupSettings.mutateAsync(true);
                queryClient.setQueryData(["settings"], (old: Record<string, unknown> | undefined) =>
                  old ? { ...old, setupWizardDismissed: true } : old,
                );
                navigate({ to: "/search", search: { q: "", help: undefined, settings: undefined, inspector: undefined } });
              }}
              dark={dark}
            >
              Skip
            </Button>
          </div>
          <div>
            {step > 0 && step < STEPS.length - 1 && (
              <Button
                onClick={() => setStep((s) => s + 1)}
                disabled={!canProceed()}
              >
                Next
              </Button>
            )}
            {step === STEPS.length - 1 && (
              <Button onClick={handleCreate} disabled={creating}>
                {creating ? "Creating..." : "Create"}
              </Button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

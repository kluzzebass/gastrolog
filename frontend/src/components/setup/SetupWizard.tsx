import { useState, useReducer, useEffect } from "react";
import { useNavigate } from "@tanstack/react-router";
import { useThemeSync } from "../../hooks/useThemeSync";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useToast } from "../Toast";
import { configClient } from "../../api/client";
import { usePutSettings, useGenerateName } from "../../api/hooks/useConfig";
import { useQueryClient } from "@tanstack/react-query";
import { PrimaryButton, GhostButton } from "../settings/Buttons";
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
  const putSettings = usePutSettings();

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

    try {
      const filterId = crypto.randomUUID();
      const vaultId = crypto.randomUUID();
      const ingesterId = crypto.randomUUID();

      // 1. Create filter (catch-all)
      await configClient.putFilter({
        config: {
          id: filterId,
          name: "catch-all",
          expression: "*",
        },
      });

      // 2. Create rotation policy (if any fields are set)
      let rotationId = "";
      if (hasRotation) {
        rotationId = crypto.randomUUID();
        await configClient.putRotationPolicy({
          config: {
            id: rotationId,
            name: rotationName,
            maxAgeSeconds: parseDurationToSeconds(rotation.maxAge),
            maxBytes: rotationMaxBytes,
            maxRecords: rotationMaxRecords,
            cron: rotation.cron,
          },
        });
      }

      // 3. Create retention policy (if any fields are set)
      let retentionId = "";
      if (hasRetention) {
        retentionId = crypto.randomUUID();
        await configClient.putRetentionPolicy({
          config: {
            id: retentionId,
            name: retentionName,
            maxChunks: retentionMaxChunks,
            maxAgeSeconds: parseDurationToSeconds(retentionMaxAge),
            maxBytes: retentionMaxBytes,
          },
        });
      }

      // 4. Create vault
      const vaultParams: Record<string, string> = {};
      if (vault.type === "file" && vault.dir) {
        vaultParams["dir"] = vault.dir;
      }
      await configClient.putVault({
        config: {
          id: vaultId,
          name: vaultName,
          type: vault.type,
          enabled: true,
          filter: filterId,
          policy: rotationId,
          retentionRules: retentionId
            ? [{ retentionPolicyId: retentionId, action: "expire" }]
            : [],
          params: vaultParams,
        },
      });

      // 5. Create ingester
      await configClient.putIngester({
        config: {
          id: ingesterId,
          name: ingesterName,
          type: ingester.type,
          enabled: true,
          params: ingester.params,
        },
      });

      await putSettings.mutateAsync({ setupWizardDismissed: true });
      // refetchType: "all" forces a refetch even for inactive queries (no
      // subscribers on /setup).  Without this the cache keeps stale data and
      // SearchView's redirect fires before the queries can refetch.
      await queryClient.invalidateQueries({ queryKey: ["config"], refetchType: "all" });
      await queryClient.invalidateQueries({ queryKey: ["settings"], refetchType: "all" });
      addToast("Configuration created successfully!", "info");
      navigate({ to: "/search", search: { q: "", help: undefined, settings: undefined, inspector: undefined } });
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "Failed to create configuration";
      addToast(errorMessage, "error");
    } finally {
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
              stepClass = "bg-copper text-white";
            } else if (i < step) {
              stepClass = c(
                "bg-copper/20 text-copper cursor-pointer hover:bg-copper/30",
                "bg-copper/20 text-copper cursor-pointer hover:bg-copper/30",
              );
            } else {
              stepClass = c(
                "bg-ink-surface text-text-ghost",
                "bg-light-well text-light-text-ghost",
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
              <GhostButton
                onClick={() => setStep((s) => s - 1)}
                dark={dark}
                bordered
              >
                Back
              </GhostButton>
            )}
            <GhostButton
              onClick={async () => {
                await putSettings.mutateAsync({ setupWizardDismissed: true });
                await queryClient.invalidateQueries({ queryKey: ["settings"], refetchType: "all" });
                navigate({ to: "/search", search: { q: "", help: undefined, settings: undefined, inspector: undefined } });
              }}
              dark={dark}
            >
              Skip
            </GhostButton>
          </div>
          <div>
            {step > 0 && step < STEPS.length - 1 && (
              <PrimaryButton
                onClick={() => setStep((s) => s + 1)}
                disabled={!canProceed()}
              >
                Next
              </PrimaryButton>
            )}
            {step === STEPS.length - 1 && (
              <PrimaryButton onClick={handleCreate} disabled={creating}>
                {creating ? "Creating..." : "Create"}
              </PrimaryButton>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

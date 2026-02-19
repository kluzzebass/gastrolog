import { useState, useReducer } from "react";
import { useNavigate } from "@tanstack/react-router";
import { useThemeSync } from "../../hooks/useThemeSync";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useToast } from "../Toast";
import { configClient } from "../../api/client";
import { useQueryClient } from "@tanstack/react-query";
import { PrimaryButton, GhostButton } from "../settings/Buttons";
import { WelcomeStep } from "./WelcomeStep";
import { StoreStep, type StoreData } from "./StoreStep";
import {
  PoliciesStep,
  parseDurationToSeconds,
  type RotationData,
  type RetentionData,
} from "./PoliciesStep";
import { IngesterStep, type IngesterData } from "./IngesterStep";
import { ReviewStep } from "./ReviewStep";

const STEPS = ["Welcome", "Store", "Policies", "Ingester", "Review"] as const;

// -- Reducer for wizard step data --

interface WizardDataState {
  store: StoreData;
  rotation: RotationData;
  retention: RetentionData;
  ingester: IngesterData;
}

const wizardDataInitial: WizardDataState = {
  store: { name: "default", type: "file", dir: "" },
  rotation: { name: "default", maxAge: "", maxBytes: "", maxRecords: "", cron: "" },
  retention: { name: "default", maxChunks: "", maxAge: "", maxBytes: "" },
  ingester: { name: "", type: "", params: {} },
};

type WizardDataAction =
  | { type: "setStore"; value: StoreData }
  | { type: "setRotation"; value: RotationData }
  | { type: "setRetention"; value: RetentionData }
  | { type: "setIngester"; value: IngesterData };

function wizardDataReducer(state: WizardDataState, action: WizardDataAction): WizardDataState {
  switch (action.type) {
    case "setStore":
      return { ...state, store: action.value };
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

  const [step, setStep] = useState(0);
  const [creating, setCreating] = useState(false);

  // Step data
  const [wizardData, dispatchData] = useReducer(wizardDataReducer, wizardDataInitial);
  const { store, rotation, retention, ingester } = wizardData;
  const setStore = (v: StoreData) => dispatchData({ type: "setStore", value: v });
  const setRotation = (v: RotationData) => dispatchData({ type: "setRotation", value: v });
  const setRetention = (v: RetentionData) => dispatchData({ type: "setRetention", value: v });
  const setIngester = (v: IngesterData) => dispatchData({ type: "setIngester", value: v });

  const canProceed = () => {
    switch (step) {
      case 0: return true; // Welcome
      case 1: // Store
        if (!store.name.trim()) return false;
        if (store.type === "file" && !store.dir.trim()) return false;
        return true;
      case 2: return true; // Policies (defaults are fine)
      case 3: return !!ingester.type; // Need a type selected
      case 4: return true; // Review
      default: return false;
    }
  };

  const handleCreate = async () => {
    setCreating(true);

    // Extract conditional expressions before try/catch for React Compiler compatibility
    const hasRotation = rotation.maxAge || rotation.maxBytes || rotation.maxRecords || rotation.cron;
    const rotationMaxBytes = rotation.maxBytes ? BigInt(rotation.maxBytes) : BigInt(0);
    const rotationMaxRecords = rotation.maxRecords ? BigInt(rotation.maxRecords) : BigInt(0);
    const rotationName = rotation.name || "default";

    const hasRetention = retention.maxChunks || retention.maxAge || retention.maxBytes;
    const retentionMaxChunks = retention.maxChunks ? BigInt(retention.maxChunks) : BigInt(0);
    const retentionMaxAge = retention.maxAge || "";
    const retentionMaxBytes = retention.maxBytes ? BigInt(retention.maxBytes) : BigInt(0);
    const retentionName = retention.name || "default";

    const storeName = store.name || "default";
    const ingesterName = ingester.name || ingester.type;

    try {
      const filterId = crypto.randomUUID();
      const storeId = crypto.randomUUID();
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

      // 4. Create store
      const storeParams: Record<string, string> = {};
      if (store.type === "file" && store.dir) {
        storeParams["dir"] = store.dir;
      }
      await configClient.putStore({
        config: {
          id: storeId,
          name: storeName,
          type: store.type,
          enabled: true,
          filter: filterId,
          policy: rotationId,
          retention: retentionId,
          params: storeParams,
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

      localStorage.removeItem("setup_skipped");
      await queryClient.invalidateQueries({ queryKey: ["config"] });
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
          {STEPS.map((label, i) => (
            <div key={label} className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => i < step && setStep(i)}
                disabled={i >= step}
                className={`w-7 h-7 rounded-full flex items-center justify-center text-[0.75em] font-mono font-medium transition-colors ${
                  i === step
                    ? "bg-copper text-white"
                    : i < step
                      ? c(
                          "bg-copper/20 text-copper cursor-pointer hover:bg-copper/30",
                          "bg-copper/20 text-copper cursor-pointer hover:bg-copper/30",
                        )
                      : c(
                          "bg-ink-surface text-text-ghost",
                          "bg-light-well text-light-text-ghost",
                        )
                }`}
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
          ))}
        </div>

        {/* Step content */}
        <div className="px-6 py-5 max-h-[60vh] overflow-y-auto app-scroll">
          {step === 0 && (
            <WelcomeStep dark={dark} onNext={() => setStep(1)} />
          )}
          {step === 1 && (
            <StoreStep dark={dark} data={store} onChange={setStore} />
          )}
          {step === 2 && (
            <PoliciesStep
              dark={dark}
              rotation={rotation}
              retention={retention}
              onRotationChange={setRotation}
              onRetentionChange={setRetention}
            />
          )}
          {step === 3 && (
            <IngesterStep dark={dark} data={ingester} onChange={setIngester} />
          )}
          {step === 4 && (
            <ReviewStep
              dark={dark}
              store={store}
              rotation={rotation}
              retention={retention}
              ingester={ingester}
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
                onClick={() => setStep(step - 1)}
                dark={dark}
                bordered
              >
                Back
              </GhostButton>
            )}
            <GhostButton
              onClick={() => {
                localStorage.setItem("setup_skipped", "1");
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
                onClick={() => setStep(step + 1)}
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

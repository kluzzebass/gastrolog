import { FormField, TextInput } from "./FormField";
import { Checkbox } from "./Checkbox";
import { NodeMultiSelect, NODE_ASSIGNMENT_HELP_REF } from "./NodeMultiSelect";

const SINGLETON_LABEL = "Singleton (run on one node with automatic failover)";

/** The singleton checkbox's label, carrying the reason when the type cannot use it. */
export function singletonLabel(singletonSupported: boolean): string {
  return singletonSupported
    ? SINGLETON_LABEL
    : `${SINGLETON_LABEL} — not available for this type: it reads a node-local source or binds a port on every assigned node`;
}

/**
 * Shared header controls for every ingester form (both create and edit):
 * Name, Enabled, Nodes, and Singleton. Extracted because the four-field
 * block appeared twice with parallel props and was drifting whenever one
 * side was touched without the other.
 */
export function IngesterCommonFields({
  name,
  namePlaceholder,
  onNameChange,
  enabled,
  onEnabledChange,
  nodeIds,
  onNodeIdsChange,
  allNodes,
  onAllNodesChange,
  singleton,
  onSingletonChange,
  singletonSupported,
  dark,
}: Readonly<{
  name: string;
  namePlaceholder?: string;
  onNameChange: (v: string) => void;
  enabled: boolean;
  onEnabledChange: (v: boolean) => void;
  nodeIds: string[];
  onNodeIdsChange: (v: string[]) => void;
  allNodes: boolean;
  onAllNodesChange: (v: boolean) => void;
  singleton: boolean;
  onSingletonChange: (v: boolean) => void;
  singletonSupported: boolean;
  dark: boolean;
}>) {
  return (
    <>
      <FormField label="Name" dark={dark}>
        <TextInput
          value={name}
          onChange={onNameChange}
          placeholder={namePlaceholder}
          dark={dark}
        />
      </FormField>
      <Checkbox
        checked={enabled}
        onChange={onEnabledChange}
        label="Enabled"
        dark={dark}
      />
      <NodeMultiSelect
        value={nodeIds}
        allNodes={allNodes}
        onValueChange={onNodeIdsChange}
        onAllNodesChange={onAllNodesChange}
        dark={dark}
      />
      <Checkbox
        checked={singletonSupported && singleton}
        onChange={onSingletonChange}
        label={singletonLabel(singletonSupported)}
        helpRef={NODE_ASSIGNMENT_HELP_REF}
        disabled={!singletonSupported}
        dark={dark}
      />
    </>
  );
}

import { FormField, TextInput } from "./FormField";
import { Checkbox } from "./Checkbox";
import { NodeMultiSelect } from "./NodeMultiSelect";

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
        onChange={onNodeIdsChange}
        dark={dark}
      />
      {singletonSupported && (
        <Checkbox
          checked={singleton}
          onChange={onSingletonChange}
          label="Singleton (run on one node with automatic failover)"
          dark={dark}
        />
      )}
    </>
  );
}

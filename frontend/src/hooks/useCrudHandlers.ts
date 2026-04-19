import { useToast } from "../components/Toast";

interface CrudOptions<TEdit, TSaveArgs, TDeleteArgs = string> {
  mutation: { mutateAsync: (args: TSaveArgs) => Promise<unknown>; isPending: boolean };
  deleteMutation: {
    mutateAsync: (args: TDeleteArgs) => Promise<unknown>;
    isPending: boolean;
  };
  label: string;
  onSaveTransform: (id: string, edit: TEdit) => TSaveArgs;
  onDeleteTransform?: (id: string) => TDeleteArgs;
  onDeleteCheck?: (id: string) => string | null;
  onDeleteSuccess?: (id: string) => void;
  clearEdit?: (id: string) => void;
}

export function useCrudHandlers<TEdit, TSaveArgs, TDeleteArgs = string>({
  mutation,
  deleteMutation,
  label,
  onSaveTransform,
  onDeleteTransform,
  onDeleteCheck,
  onDeleteSuccess,
  clearEdit,
}: CrudOptions<TEdit, TSaveArgs, TDeleteArgs>) {
  const { addToast } = useToast();

  const handleSave = async (id: string, edit: TEdit) => {
    try {
      const args = onSaveTransform(id, edit);
      await mutation.mutateAsync(args);
      // Do not eagerly clear edit state on success. Some backend mutations
      // acknowledge before the returned system snapshot advances; if we clear
      // immediately, fields can snap back to stale defaults and then forward
      // again once WatchSystem/GetSystem catches up.
      //
      // useEditState will naturally drop stale edits when defaults change,
      // so keeping the edit here avoids visible bounce without long-lived drift.
      addToast(`${label} "${id}" updated`, "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : `Failed to update ${label.toLowerCase()}`, "error");
    }
  };

  const handleDelete = async (id: string) => {
    if (onDeleteCheck) {
      const warning = onDeleteCheck(id);
      if (warning) {
        addToast(warning, "warn");
        return;
      }
    }
    try {
      const deleteArgs = onDeleteTransform ? onDeleteTransform(id) : (id as TDeleteArgs);
      await deleteMutation.mutateAsync(deleteArgs);
      if (onDeleteSuccess) {
        onDeleteSuccess(id);
      } else {
        addToast(`${label} "${id}" deleted`, "info");
      }
    } catch (err: unknown) {
      addToast(
        err instanceof Error ? err.message : `Failed to delete ${label.toLowerCase()}`,
        "error",
      );
    }
  };

  return { handleSave, handleDelete };
}

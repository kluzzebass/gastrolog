import { useToast } from "../components/Toast";

interface CrudOptions<TEdit> {
  mutation: { mutateAsync: (args: any) => Promise<any>; isPending: boolean };
  deleteMutation: {
    mutateAsync: (args: any) => Promise<any>;
    isPending: boolean;
  };
  label: string;
  onSaveTransform: (id: string, edit: TEdit) => any;
  onDeleteCheck?: (id: string) => string | null;
  onDeleteSuccess?: (id: string) => void;
  clearEdit?: (id: string) => void;
}

export function useCrudHandlers<TEdit>({
  mutation,
  deleteMutation,
  label,
  onSaveTransform,
  onDeleteCheck,
  onDeleteSuccess,
  clearEdit,
}: CrudOptions<TEdit>) {
  const { addToast } = useToast();

  const handleSave = async (id: string, edit: TEdit) => {
    try {
      const args = onSaveTransform(id, edit);
      await mutation.mutateAsync(args);
      clearEdit?.(id);
      addToast(`${label} "${id}" updated`, "info");
    } catch (err: any) {
      addToast(err.message ?? `Failed to update ${label.toLowerCase()}`, "error");
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
      await deleteMutation.mutateAsync(id);
      if (onDeleteSuccess) {
        onDeleteSuccess(id);
      } else {
        addToast(`${label} "${id}" deleted`, "info");
      }
    } catch (err: any) {
      addToast(
        err.message ?? `Failed to delete ${label.toLowerCase()}`,
        "error",
      );
    }
  };

  return { handleSave, handleDelete };
}

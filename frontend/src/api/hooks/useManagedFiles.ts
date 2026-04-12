import { systemClient } from "../client";
import { useSystemMutation } from "./useSystem";

export function useDeleteManagedFile() {
  return useSystemMutation(async (id: string) => {
    await systemClient.deleteManagedFile({ id });
  });
}

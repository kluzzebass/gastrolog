import { systemClient } from "../client";
import { useSystemMutation } from "./useSystem";
import { decode } from "../glid";

export function useDeleteManagedFile() {
  return useSystemMutation(async (id: string) => {
    await systemClient.deleteManagedFile({ id: decode(id) });
  });
}

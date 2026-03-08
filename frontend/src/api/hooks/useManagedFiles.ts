import { configClient } from "../client";
import { useConfigMutation } from "./useConfig";

export function useDeleteManagedFile() {
  return useConfigMutation(async (id: string) => {
    await configClient.deleteManagedFile({ id });
  });
}

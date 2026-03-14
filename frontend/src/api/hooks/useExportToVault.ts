import { useMutation } from "@tanstack/react-query";
import { queryClient as rpcClient } from "../client";

interface ExportToVaultArgs {
  expression: string;
  target: string;
}

export function useExportToVault() {
  return useMutation({
    mutationFn: async ({ expression, target }: ExportToVaultArgs) => {
      const response = await rpcClient.exportToVault({ expression, target });
      return response.jobId;
    },
  });
}

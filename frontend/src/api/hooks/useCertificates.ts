import { useQuery } from "@tanstack/react-query";
import { configClient } from "../client";
import { useConfigMutation } from "./useConfig";

export function useCertificates() {
  return useQuery({
    queryKey: ["certificates"],
    queryFn: async () => {
      const response = await configClient.listCertificates({});
      return response;
    },
  });
}

export function useCertificate(id: string | null) {
  return useQuery({
    queryKey: ["certificate", id],
    queryFn: async () => {
      if (!id) return null;
      const response = await configClient.getCertificate({ id });
      return response;
    },
    enabled: !!id,
  });
}

export function usePutCertificate() {
  return useConfigMutation(
    async (args: {
      id: string;
      name: string;
      certPem?: string;
      keyPem?: string;
      certFile?: string;
      keyFile?: string;
      setAsDefault?: boolean;
    }) => {
      return configClient.putCertificate({
        id: args.id,
        name: args.name,
        certPem: args.certPem ?? "",
        keyPem: args.keyPem ?? "",
        certFile: args.certFile ?? "",
        keyFile: args.keyFile ?? "",
        setAsDefault: args.setAsDefault ?? false,
      });
    },
    [["certificates"], ["settings"]],
  );
}

export function useDeleteCertificate() {
  return useConfigMutation(
    async (id: string) => {
      return configClient.deleteCertificate({ id });
    },
    [["certificates"], ["settings"]],
  );
}

import { useQuery } from "@tanstack/react-query";
import { systemClient } from "../client";
import { useSystemMutation } from "./useSystem";
import { decode } from "../glid";

export function useCertificates() {
  return useQuery({
    queryKey: ["certificates"],
    queryFn: async () => {
      const response = await systemClient.listCertificates({});
      return response;
    },
  });
}

export function useCertificate(id: string | null) {
  return useQuery({
    queryKey: ["certificate", id],
    queryFn: async () => {
      if (!id) return null;
      const response = await systemClient.getCertificate({ id: decode(id) });
      return response;
    },
    enabled: !!id,
  });
}

export function usePutCertificate() {
  return useSystemMutation(
    async (args: {
      id: string;
      name: string;
      certPem?: string;
      keyPem?: string;
      certFile?: string;
      keyFile?: string;
      setAsDefault?: boolean;
    }) => {
      return systemClient.putCertificate({
        id: decode(args.id),
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
  return useSystemMutation(
    async (id: string) => {
      return systemClient.deleteCertificate({ id: decode(id) });
    },
    [["certificates"], ["settings"]],
  );
}

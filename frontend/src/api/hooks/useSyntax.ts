import { useQuery } from "@tanstack/react-query";
import { queryClient } from "../client";

export interface SyntaxKeywords {
  directives: Set<string>;
  pipeKeywords: Set<string>;
  pipeFunctions: Set<string>;
  lookupTables: Set<string>;
}

export function useSyntax() {
  return useQuery({
    queryKey: ["syntax"],
    queryFn: async (): Promise<SyntaxKeywords> => {
      const response = await queryClient.getSyntax({});
      return {
        directives: new Set(response.directives),
        pipeKeywords: new Set(response.pipeKeywords),
        pipeFunctions: new Set(response.pipeFunctions),
        lookupTables: new Set(response.lookupTables),
      };
    },
    staleTime: Infinity, // Never refetch — keyword sets don't change at runtime.
  });
}

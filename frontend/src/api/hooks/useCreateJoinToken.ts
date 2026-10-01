import { useMutation } from "@tanstack/react-query";
import { lifecycleClient } from "../client";

/**
 * Mint a join token for admitting a new node.
 *
 * Tokens expire, so there is no standing one to read: the cluster holds the
 * key that signs them and issues one when asked. That makes this a mutation
 * rather than a query — it is an action with a consequence, and repeating it
 * produces a different answer each time.
 */
export function useCreateJoinToken() {
  return useMutation({
    mutationFn: async (args?: { ttlSeconds?: bigint }) => {
      const resp = await lifecycleClient.createJoinToken({
        ttlSeconds: args?.ttlSeconds ?? BigInt(0),
      });
      return { token: resp.joinToken, expiresAt: resp.expiresAtUnix };
    },
  });
}

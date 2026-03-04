import { ConnectError } from "@connectrpc/connect";

/** Extract a human-readable message from an unknown error, with a fallback. */
export function extractMessage(err: unknown, fallback: string): string {
  if (err instanceof Error) return err.message;
  return fallback;
}

/** Like extractMessage, but prefers ConnectError.rawMessage (no gRPC code prefix). */
export function connectMessage(err: unknown, fallback: string): string {
  if (err instanceof ConnectError) return err.rawMessage;
  if (err instanceof Error) return err.message;
  return fallback;
}

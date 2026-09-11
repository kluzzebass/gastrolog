import { Code, ConnectError } from "@connectrpc/connect";

/**
 * shouldReconnectAfter decides whether a dropped long-lived stream is worth
 * reopening.
 *
 * Almost everything is: a restart, a proxy timeout, a lost network, an
 * expired token the refresh will fix. A refusal is not. The caller lacks the
 * role the stream requires, and no amount of waiting changes that, so
 * reconnecting asks the same question every thirty seconds for as long as
 * the tab stays open — against a server that has already said no.
 *
 * Shared by every watch hook so the three of them cannot drift apart on what
 * counts as permanent.
 */
export function shouldReconnectAfter(err: unknown): boolean {
  return !(err instanceof ConnectError && err.code === Code.PermissionDenied);
}

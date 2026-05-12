// Branded entity-ID type for the frontend model layer.
//
// Proto wire IDs arrive as `Uint8Array` (16 raw GLID bytes). Most components
// today call `encode()` inline to convert to the canonical 26-char base32hex
// string, then pass that string around alongside un-converted Uint8Array
// values from other RPCs. The mix is the source of recurring "encode(x) ===
// encode(y)" comparisons and silent bugs when one side forgets the conversion.
//
// `EntityID` is a branded alias over `string`: it carries zero runtime cost
// (a brand is type-only) but the compiler rejects mixing it with arbitrary
// strings or with `Uint8Array`. The only ways into the type are the helpers
// below, which force the conversion at the boundary instead of scattering it
// across components.

import { encode, decode, isZero } from "../glid";

declare const entityIDBrand: unique symbol;

/**
 * A canonical, string-encoded GLID. Created only via {@link idFromBytes},
 * {@link asEntityID}, or the empty-id sentinel `EMPTY_ID`.
 */
export type EntityID = string & { readonly [entityIDBrand]: true };

/** The empty/zero ID. Distinct from "unknown" — represents a missing or zero GLID. */
export const EMPTY_ID = "" as EntityID;

/** Convert raw proto bytes to an EntityID. Empty / zero-byte inputs → EMPTY_ID. */
export function idFromBytes(b: Uint8Array | undefined): EntityID {
  if (!b || b.length === 0 || isZero(b)) return EMPTY_ID;
  return encode(b) as EntityID;
}

/**
 * Convert an EntityID back to proto bytes. EMPTY_ID round-trips to a 16-byte
 * zero array, matching how PutXxx mutations send a fresh-id placeholder.
 */
export function idToBytes(id: EntityID): Uint8Array {
  return decode(id);
}

/**
 * Tag an existing string as an EntityID. Use sparingly — prefer
 * `idFromBytes` for proto inputs. Valid uses: URL params, dcat IDs from
 * config files, IDs already in canonical string form on the wire (e.g.
 * peer IDs in stats broadcasts).
 */
export function asEntityID(s: string): EntityID {
  return s as EntityID;
}

/**
 * Typed equality. Equivalent to `===` at runtime; exists so the compiler
 * enforces that both sides have already been converted out of Uint8Array.
 */
export function eqID(a: EntityID, b: EntityID): boolean {
  return a === b;
}

/** Convenience predicate. */
export function isEmptyID(id: EntityID): boolean {
  return id === EMPTY_ID;
}

// glid.ts — frontend GLID encoding/decoding.
// Matches backend internal/glid: base32hex (RFC 4648), lowercase, no padding.
// 16 raw bytes ↔ 26-char string, lexicographically sortable by UUIDv7 time.

const ALPHABET = "0123456789abcdefghijklmnopqrstuv";
const DECODE_MAP = new Uint8Array(128);
for (let i = 0; i < ALPHABET.length; i++) {
  DECODE_MAP[ALPHABET.codePointAt(i)!] = i;
  DECODE_MAP[ALPHABET.toUpperCase().codePointAt(i)!] = i;
}

const SIZE = 16;
const ENCODED_LEN = 26;
const ZERO = new Uint8Array(SIZE);

/** Encode 16 raw bytes to a 26-char base32hex string (lowercase, no padding). */
export function encode(bytes: Uint8Array): string {
  if (bytes.length < SIZE) return "";
  let allZero = true;
  for (let i = 0; i < SIZE; i++) {
    if (bytes[i] !== 0) { allZero = false; break; }
  }
  if (allZero) return "";

  // Base32hex: 5 bytes → 8 chars. 16 bytes = 3×5 + 1 remainder byte.
  const chars: string[] = [];
  let i = 0;
  // Process 3 full 5-byte groups (15 bytes → 24 chars)
  for (let g = 0; g < 3; g++) {
    const b0 = bytes[i++]!, b1 = bytes[i++]!, b2 = bytes[i++]!, b3 = bytes[i++]!, b4 = bytes[i++]!;
    chars.push(
      ALPHABET[(b0 >> 3)]!,
      ALPHABET[((b0 & 0x07) << 2) | (b1 >> 6)]!,
      ALPHABET[(b1 >> 1) & 0x1F]!,
      ALPHABET[((b1 & 0x01) << 4) | (b2 >> 4)]!,
      ALPHABET[((b2 & 0x0F) << 1) | (b3 >> 7)]!,
      ALPHABET[(b3 >> 2) & 0x1F]!,
      ALPHABET[((b3 & 0x03) << 3) | (b4 >> 5)]!,
      ALPHABET[b4 & 0x1F]!,
    );
  }
  // Remaining 1 byte → 2 chars (no padding)
  const b0 = bytes[i]!;
  chars.push(
    ALPHABET[(b0 >> 3)]!,
    ALPHABET[((b0 & 0x07) << 2)]!,
  );

  return chars.join("");
}

/** Decode a 26-char base32hex string to 16 raw bytes. Returns zero bytes for empty/invalid input. */
export function decode(s: string): Uint8Array<ArrayBuffer> {
  if (s?.length !== ENCODED_LEN) return new Uint8Array(ZERO);

  const out = new Uint8Array(SIZE);
  let oi = 0;
  let si = 0;

  // 3 full 5-byte groups from 24 chars
  for (let g = 0; g < 3; g++) {
    const c0 = DECODE_MAP[s.codePointAt(si++)!]!;
    const c1 = DECODE_MAP[s.codePointAt(si++)!]!;
    const c2 = DECODE_MAP[s.codePointAt(si++)!]!;
    const c3 = DECODE_MAP[s.codePointAt(si++)!]!;
    const c4 = DECODE_MAP[s.codePointAt(si++)!]!;
    const c5 = DECODE_MAP[s.codePointAt(si++)!]!;
    const c6 = DECODE_MAP[s.codePointAt(si++)!]!;
    const c7 = DECODE_MAP[s.codePointAt(si++)!]!;
    out[oi++] = (c0 << 3) | (c1 >> 2);
    out[oi++] = ((c1 & 0x03) << 6) | (c2 << 1) | (c3 >> 4);
    out[oi++] = ((c3 & 0x0F) << 4) | (c4 >> 1);
    out[oi++] = ((c4 & 0x01) << 7) | (c5 << 2) | (c6 >> 3);
    out[oi++] = ((c6 & 0x07) << 5) | c7;
  }
  // Remaining 2 chars → 1 byte
  const c0 = DECODE_MAP[s.codePointAt(si++)!]!;
  const c1 = DECODE_MAP[s.codePointAt(si)!]!;
  out[oi] = (c0 << 3) | (c1 >> 2);

  return out;
}

/** Check if bytes represent the zero GLID. */
export function isZero(bytes: Uint8Array): boolean {
  if (bytes.length < SIZE) return true;
  for (let i = 0; i < SIZE; i++) {
    if (bytes[i] !== 0) return false;
  }
  return true;
}

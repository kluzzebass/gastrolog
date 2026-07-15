# Audit finding → dcat issue map

Epic: **gastrolog-2p313**  
Run `dcat list gastrolog-2p313` for live tree.

## Remediation sub-epics

| Epic | ID | Focus |
|------|-----|-------|
| A | **gastrolog-18k9l** | Query index unification |
| B | **gastrolog-2e3mt** | Vault-ctl read-after-write |
| C | **gastrolog-64ipe** | FSM-grounded chunk metadata |
| D | **gastrolog-36wys** | Pipeline event durability (root-cause fixes) |
| E | **gastrolog-8gmd0** | Compensator retirement (sweeps) |
| F | **gastrolog-3471q** | Layering & cluster-first transport |

## Anchor issues (pre-existing, reparented)

| Issue | Was under | Now under |
|-------|-----------|-----------|
| gastrolog-2o9e9 | gastrolog-q9tek | gastrolog-18k9l (A) |
| gastrolog-2i62e | gastrolog-2p313 | gastrolog-8gmd0 (E) |
| gastrolog-5vwav | gastrolog-2p313 | gastrolog-8gmd0 (E) |
| gastrolog-12gue | gastrolog-2p313 | gastrolog-8gmd0 (E) |
| gastrolog-4l24u | gastrolog-2p313 | gastrolog-2e3mt (B) |
| gastrolog-3fu9t | gastrolog-2p313 | gastrolog-8gmd0 (E) |
| gastrolog-576bm | gastrolog-2p313 | gastrolog-8gmd0 (E) |
| gastrolog-4gp8h | gastrolog-2p313 | gastrolog-3471q (F) |
| gastrolog-2bv1x | gastrolog-2p313 | gastrolog-3471q (F) |
| gastrolog-5kdzj | gastrolog-2p313 | gastrolog-3471q (F) |

## Sweep → dcat

| Sweep | dcat |
|-------|------|
| sweep-001 | gastrolog-2i62e |
| sweep-002 | gastrolog-5vwav |
| sweep-003 | gastrolog-12gue |
| sweep-004 | gastrolog-12gue |
| sweep-005 | gastrolog-29xpy |
| sweep-006 | gastrolog-3oram |
| sweep-007 | gastrolog-1go57 |
| sweep-008 | gastrolog-3klg1 |
| sweep-009 | gastrolog-1loe9 (see gastrolog-9ohip closed) |
| sweep-010 | gastrolog-3fu9t |
| sweep-011 | gastrolog-3sdnn |
| sweep-012 | gastrolog-1a18r |
| sweep-013 | gastrolog-576bm |
| sweep-014 | — (legitimate policy tick) |
| sweep-015 | gastrolog-48o1r |
| sweep-016 | gastrolog-4vg17 |
| sweep-017 | gastrolog-15nn1 |
| sweep-018 | gastrolog-2iwai |

## P0/P1 findings → dcat

Every P0/P1 audit finding has a task under the sub-epic above. Title prefix matches finding ID (`audit-storage-003`, `sweep-005`, etc.). Search: `dcat list gastrolog-18k9l` (and B–F).

Closed pre-existing issues referenced in descriptions: gastrolog-3ukgz, gastrolog-4trvb, gastrolog-9ohip, gastrolog-2idw8, gastrolog-5d5a3, gastrolog-2nxij.

P2 findings remain memo-only unless promoted.

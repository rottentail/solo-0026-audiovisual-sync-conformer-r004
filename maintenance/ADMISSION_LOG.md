# Admission Log

One row per gate evaluation. Attach the JSON report (`-report`) to the
release review it belongs to. Verdicts: `ADMIT` (exit 0),
`ADMIT WITH SIGN-OFF` (exit 2), `REJECT` (exit 1).

| date | candidate | gate run | verdict | key differences | decision |
|---|---|---|---|---|---|
| 2026-11-13 | baseline `2859238` self-check | `syncversiongate verify` against corpus v1 | ADMIT (0) | 5 byte-level (per-run receipt timestamp in bundle digests) | corpus v1 blessed as the admission contract |
| 2026-11-13 | demo variant A: WAV 18-byte `fmt ` chunk, Y4M frame comment token | build + 3 flowchecks pass; gate run in variant tree | ADMIT (0) | 30 byte-level (output + receipt digests); decoded samples/frames identical | released-equivalent: bytes changed, audiovisual meaning unchanged |
| 2026-11-13 | demo variant B: flash threshold 0.55 → 0.80 | build + 3 flowchecks pass; gate run in variant tree | REJECT (1) | 11 semantic: `ramp-flash` video event 375→416 ms, offset 75→34 ms, applied 3600→1632 samples, aligned-audio content hash changed; historical bundle re-measures at −41 ms | blocked: files stay readable but sync semantics changed; would need the intentional-semantic-release procedure |
| 2026-11-13 | demo variant C: video prominence ÷80 → ÷120 | build + 3 flowchecks pass; gate run in variant tree | ADMIT WITH SIGN-OFF (2) | 2 advisory: `weak-flash` confidence 0.875→0.75, video prominence 0.75→0.5 | sign-off required; re-bless golden after acceptance |

## Entry template

```text
| YYYY-MM-DD | <commit / change summary> | <commands run> | <verdict (exit)> | <counts and fields> | <decision and signer> |
```

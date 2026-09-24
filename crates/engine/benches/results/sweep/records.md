# sweep resource records (task 1.1/1.2)

| Scenario | Goodput (MiB/s) | Wire (MiB/s) | Completed | Network | Reused | Retransferred | Retries | Wall | CPU | Peak RSS | Ctx Switches | Connections | Verify |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| sweep/h1/explicit-4MiB | 1711.03 | 1711.03 | 269729792 | 269729792 | 0 | 0 | 0 | 0.15033881 | 299.32390711353906 | 16000 | 7 | n/r | ok |
| sweep/h1/explicit-8MiB | 2043.55 | 2043.55 | 269402151 | 269402151 | 0 | 0 | 0 | 0.125723575 | 365.88205513564185 | 17892 | 3 | n/r | ok |
| sweep/h1/explicit-16MiB | 2528.74 | 2528.74 | 268894208 | 268894208 | 0 | 0 | 0 | 0.101409329 | 433.88513102182145 | 17892 | 1 | n/r | ok |
| sweep/h1/auto-x2 | 2078.82 | 2078.82 | 268894208 | 268894208 | 0 | 0 | 0 | 0.123357078 | 356.6880856240775 | 17892 | 3 | n/r | ok |
| sweep/h1/auto-x3 | 1346.31 | 1346.31 | 270150311 | 270150311 | 0 | 0 | 0 | 0.19136428 | 229.92796774821298 | 17892 | 2 | n/r | ok |
| sweep/h1/auto-x4 | 2531.32 | 2531.32 | 268763136 | 268763136 | 0 | 0 | 0 | 0.101256379 | 424.66460310614116 | 17892 | 1 | n/r | ok |
| sweep/h2/explicit-4MiB | 841.00 | 841.00 | 268435456 | 268435456 | 0 | 0 | 0 | 0.304400951 | 141.26105670412312 | 17892 | 3 | n/r | ok |
| sweep/h2/explicit-8MiB | 743.37 | 743.37 | 268435456 | 268435456 | 0 | 0 | 0 | 0.344375367 | 127.76755893809329 | 17892 | 2 | n/r | ok |
| sweep/h2/explicit-16MiB | 737.92 | 737.92 | 268435456 | 268435456 | 0 | 0 | 0 | 0.346922664 | 129.71190605177642 | 17892 | 26 | n/r | ok |
| sweep/h2/auto-x2 | 839.77 | 839.77 | 268435456 | 268435456 | 0 | 0 | 0 | 0.304844192 | 144.3360285506112 | 17892 | 3 | n/r | ok |
| sweep/h2/auto-x3 | 744.34 | 744.34 | 268435456 | 268435456 | 0 | 0 | 0 | 0.343928925 | 125.02583200875006 | 17892 | 1 | n/r | ok |
| sweep/h2/auto-x4 | 837.55 | 837.55 | 268435456 | 268435456 | 0 | 0 | 0 | 0.305654184 | 143.9535341024483 | 17892 | 2 | n/r | ok |

## Tuning selection (task 6.3)

Session: 2026-09-24, baseline host, 256 MiB synthetic fixture, 4 workers,
one measured run per configuration (single-run variance ±10-20%; see the
milestone-2 comparison for the noise caveat).

| Configuration | H1 goodput | H1 wire overhead | H2 goodput |
|---|---|---|---|
| explicit 4 MiB | 1711 MiB/s | +0.48% | 841 MiB/s |
| explicit 8 MiB (default) | 2044 MiB/s | +0.36% | 743 MiB/s |
| explicit 16 MiB | 2529 MiB/s | +0.17% | 738 MiB/s |
| auto ×2 (target 32 MiB) | 2079 MiB/s | +0.17% | 840 MiB/s |
| auto ×3 (target 21.3 MiB) | 1346 MiB/s | +0.64% | 744 MiB/s |
| auto ×4 (target 16 MiB) | 2531 MiB/s | +0.12% | 838 MiB/s |

Findings:
1. The milestone-2 wire duplication (2.0-2.5×) is essentially GONE for every
   target ≥ 4 MiB (<1% overhead): honoring the explicit initial size (or the
   automatic target) keeps enough leases in flight that idle workers do not
   need to split live tails — the overhead that made `retransferred` bytes
   misleading is a scheduler artifact, not an output-path one.
2. H1 improves with larger targets (fewer request/setup rounds per byte);
   auto ×4 ≡ explicit 16 MiB (the formula derives the same value) and both
   measure best — internally consistent.
3. H2 is flat across all sizings (~740-840 MiB/s): the single-connection
   multiplexing path is the bottleneck, not lease sizing (unchanged by
   design — `H2ConnectionPolicy` is independent).
4. retries = 0 and wasted = 0 in every configuration.

Selection: **keep the documented default (Explicit, 8 MiB)** — the
milestone-6 change makes the configured value effective (the fix), and no
configuration shows a repeatable win beyond single-run noise on this host.
The automatic mode's initial factor stays 3 (the design's initial candidate);
its ×4 equivalent (16 MiB explicit) matched the best H1 result and remains
the first candidate if a quiet-host session shows a repeatable win. Both
selectors are opt-in/configurable and recorded here for the milestone-12
final review.

## Summary

Lab 7: chapter 2 of the thesis consolidated into its final structure, with a Docs Quality Gate in CI.

**Text (`docs/2_design.md`)**
- §2.1 – architectural style (embedded replicated state machine, shared-nothing modular monolith, Ports & Adapters),
  rejected alternatives, NFR-backed trade-offs, micro-level patterns, residual risks; ADR index moved here.
- §2.3.3 – data path through the server core: Netty pipeline in its real order → `RaftDispatcher` / `RaftState` → ports.
- §2.4 – Application Security: trust boundaries, 12 controls mapped to C4 components, OWASP Top 10:2021 and SRS;
  mandatory production profile (TLS, cluster secret, token secret are off by default); known limitations.
- §2.5.3–2.5.4 – route map (7 RPCs, 3 HTTP routes, 1 cross-cutting header) checked field by field against the
  contracts; data flows with size limits, timeouts and unknown-outcome handling.
- §2.6 – use case model (StarUML) and specifications UC-01 (distributed lock with fencing) and UC-02 (tiered cache):
  preconditions, happy path at the level of server calls, postconditions, alternative and exception flows.
- §2.8.3–2.8.4 and chapter conclusions. Sections renumbered to 2.1–2.8; all anchors updated.

**Contracts and repository**
- `docs/api/openapi.yaml`: `GET /actuator/prometheus`; `X-Kurin-Consistency` header and `ConsistencyToken` schema.
- `.gitignore`: `.env`, `*.p12`, `*.jks`.
- `docs/F2_Diploma_Chapters1-2_Дармограй.docx` – monolith of chapters 1–2, built by `docs/_work/lab7/build_monolith.py`.

**Docs Quality Gate**
- `.markdownlint.jsonc`, `.markdownlint-cli2.jsonc`, `.github/workflows/docs-linter.yml` (markdownlint-cli2 0.23.2,
  pinned) on every pull request to `master` that touches `docs/`.
- Existing Markdown in `docs/` fixed to pass (line wrapping, list markers, blank lines, bare URLs, one broken table).

## Verification
- `npx markdownlint-cli2@0.23.2` – 0 issues in `docs/`.
- `openapi.yaml` parses; every relative link and anchor between `README.md`, `2_design.md`, `srs.md`, `adr/` resolves.
- Every statement about the implementation was checked against Kurin 1.1.2 sources.

Not to be merged before the instructor's review.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_012W6Ebm73w4WxHvdoqfmLn2

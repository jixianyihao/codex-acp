# ACP Protocol and Session Timing Logs

## Context

`codex-acp` v1.1.0 already records the raw stdio traffic between the adapter and Codex app-server when `APP_SERVER_LOGS` is set. It does not record the ACP transport between the client and the adapter, and its session logs do not identify the duration of individual blocking stages.

## Goals

- Record raw traffic across both protocol boundaries in the existing `app-server.log` file.
- Record enough stage timings to identify which operation delays session creation, loading, or resumption.
- Keep the implementation small and preserve all protocol behavior.
- Restore the accidental `this.mo'i'de()` edit to `this.fetchAvailableModels()`.

## Non-goals

- Do not add caching or change session startup behavior.
- Do not change JSON-RPC payloads, ordering, retry behavior, or timeouts.
- Do not add a second logging backend or a new required environment variable.
- Do not instrument model inference or turn execution beyond the raw protocol traffic already captured.

## Design

### Raw protocol traffic

Continue using `APP_SERVER_LOGS` as the single enablement switch and `app-server.log` as the destination.

Use explicit direction prefixes:

- `[ACP IN]`: client to `codex-acp`
- `[ACP OUT]`: `codex-acp` to client
- `[APP_SERVER IN]`: `codex-acp` to Codex app-server
- `[APP_SERVER OUT]`: Codex app-server to `codex-acp`
- `[APP_SERVER ERR]`: app-server stderr
- `[APP_SERVER EXIT]`: app-server process exit

ACP traffic will be observed at the stdio boundary in the same pass-through style as the existing app-server logging. Logging must not consume, modify, delay, or duplicate protocol bytes on stdout.

### Session timing

Add structured `[TIMING]` records around the blocking session-open stages:

- total ACP session open
- authorization check
- skills refresh, including `skills/extraRoots/set` and `skills/list`
- MCP config conflict read when `config/read` is actually needed
- `thread/start` or `thread/resume`
- `thread/read` for load operations
- every `model/list` page and the complete model-list operation
- provider-specific account-state read when it occurs

Each completion record will include the operation name, outcome, elapsed milliseconds, and safe result metadata such as model count or whether another cursor exists. Failures will be timed and rethrown unchanged.

### Data handling

Raw protocol logging intentionally records complete ACP and app-server payloads, so it can contain prompts, file content, model configuration, and authentication-related request fields. It remains disabled unless `APP_SERVER_LOGS` is explicitly set. Documentation will warn users to protect and delete diagnostic logs when no longer needed.

Structured timing records will not add environment variables, API keys, headers, prompts, or full configuration objects beyond what the raw protocol stream already contains.

### Error handling

Logging remains best-effort. File creation or append failures are reported to stderr and must never fail an ACP or app-server request. Timing instrumentation preserves the original exception and control flow.

## Verification

- Add tests that prove ACP input and output are logged without altering the transported bytes.
- Add tests for successful and failed timed operations.
- Add a behavior test showing `model/list` page and total timing metadata.
- Run the focused tests, full test suite, typecheck, and build.

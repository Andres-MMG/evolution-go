# WhatsApp handshake version initialization

This correction is a minimal backport onto the existing deployment fork. Its lifecycle controller, client registries, startup reconnect behavior, dependencies and Compose configuration are preserved. It has not been deployed and has not been tested against a real WhatsApp session. No instance, account, credential, session database, webhook, migration or production configuration is modified by this patch preparation.

## Corrected behavior

The service resolves the WhatsApp version once, before returning from `NewWhatsmeowService` and before starting clients. It uses `store.SetWAVersion` so that the actual client handshake, registration build hash and existing device version fields use the same selected version.

Selection retains the existing precedence: a complete nonzero configured triple wins; otherwise the public WhatsApp Web revision is fetched. Partial configuration still takes the lookup path. Existing configured integer-to-uint32 conversion is unchanged. A lookup failure leaves the embedded library payload and device properties unchanged, and the startup log identifies that fallback rather than claiming a fetched version was applied.

The public lookup has a ten-second HTTP deadline and accepts only successful HTTP responses with a positive uint32 client revision. No observed incident version is hardcoded into production code. Versions in tests are synthetic fixtures.

## Concurrency and operational limits

Upstream handshake readers access process-global version state without taking a writer lock. Updating that state during individual connections or reconnects would introduce races. Initialization is therefore process-wide and immutable after the first call, including an embedded fallback after a failed lookup. A later version change or lookup retry requires a planned process restart; the operator must plan that shared-service interruption.

This deliberately replaces the previous hourly per-client version refresh. Updating the displayed device version alone did not update the handshake. The patch does not fix unrelated pre-existing per-client OS/platform property writes and does not claim that all client startup code is race-free.

## Isolated verification

Regression tests inspect the pinned library's real `Device.GetClientPayload` for registered and fresh devices, the registration build hash, configured and fetched precedence, failure fallback, concurrent initialization and repeated service construction. HTTP responses are simulated in memory. Constructor coverage uses `go-sqlmock`, not a database server. Existing poll migration calls are expected on that mock only; no schema is changed by the patch.

Linux tests must run with network disabled, no production environment or session data mounts, and the source mounted read-only. No application listener, provider connection, real account or QR pairing is needed for these tests.

Before any future deployment, separately verify the actual production image/source revision, compatibility with that revision, preserved license/notice requirements, backup and rollback procedures, and obtain explicit deployment authorization. This fork-source candidate is not evidence that the deployed binary already contains the correction or that WhatsApp will accept a live connection.

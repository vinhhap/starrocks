# LDAP User Cache Implementation Plan

Goal: add a safe, bounded LDAP user cache so StarRocks can reuse recent bind results / DN resolutions without repeated round-trips, while keeping correctness and security aligned with LDAP semantics.

## Reference (Apache Doris)
- `org.apache.doris.mysql.authenticate.ldap.LdapUserInfo`: immutable cache entry storing `userName`, `isExists`, `isSetPasswd`, `passwd`, `roles`, `lastTimeStamp`; exposes `checkTimeout()` using `LdapConfig.ldap_user_cache_timeout_s` TTL and `cloneWithPasswd()` for updating password.
- Behavior to mirror/adjust: TTL-based expiry per entry; caching negative lookups; optional password retention for revalidation; simple, thread-safe reuse of cached record.

## Proposed StarRocks Design
- **New cache entry**: `LdapUserCacheEntry` (analogous to Doris `LdapUserInfo`), fields: username, distinguishedName (resolved DN), exists flag, optional password hash/token (avoid storing raw), lastRefreshed/createdAt. Do **not** cache roles/groups here; `LDAPGroupProvider` already caches group membership.
- **Cache container**: bounded concurrent map in FE (e.g., in `AuthenticationMgr` or a dedicated `LdapUserCache` helper) with TTL eviction; support negative cache entries for misses; size cap with LRU/LFU (Guava cache or manual LinkedHashMap with locks) to prevent unbounded growth.
- **Config knobs (FE)**: `authentication_ldap_simple_user_cache_ttl_seconds`, `authentication_ldap_simple_user_cache_max_entries`, `authentication_ldap_simple_user_cache_enable` (default disabled), `authentication_ldap_simple_user_cache_store_password` (bool, default false), and optional `authentication_ldap_simple_user_cache_store_dn_only` behavior. Stay compatible with existing LDAP auth settings documented for 3.5 (server host/port, base DN, search attr, bind root DN/PWD, SSL flags) and the security integration chain.
- **Auth flow changes** (in `LDAPAuthProvider.authenticate`):
  1) Normalize username; check cache for entry and TTL; if valid and policy allows password reuse, verify provided password matches cached token/hash or re-bind when `store_password=false` but DN cached.
  2) If cache miss/expired: resolve DN (`ldapUserDN` shortcut or `findUserDNByRoot`), perform bind, then populate cache with DN and password token (if enabled) or marker that last bind succeeded at time T.
  3) Always set `ConnectContext.distinguishedName` from cached/resolved entry.
- **Password handling**: never persist raw password beyond TTL; if `store_password=false`, keep only DN+timestamp and require bind on next request after TTL; if true, store a hashed representation (e.g., SHA-256 of UTF-8 password) strictly in-memory for same-process reuse, and still force re-bind when TTL expired.
- **Thread safety**: use `ConcurrentHashMap` + `AtomicReference`/CAS updates or Guava Cache to avoid locks per login; ensure eviction runs safely from background scheduler or on-demand.
- **Negative caching**: cache non-existent users for a short TTL to cut repeated failed lookups; distinguish between "DN not found" and "bind failed" for error clarity.
- **Metrics & logging**: expose cache hit/miss/evict counters; optionally debug log DN resolutions.
- **Integration with group cache**: keep group provider cache separate; when auth succeeds, group lookup may still use `distinguishedName` set by auth.

### Compatibility with existing LDAP features (3.5 docs)
- LDAP authentication flows (`ldap_authentication`) remain unchanged except for optional cache reuse; binds/searches still follow configured server host/port/base DN/search attr/root DN/root pwd/SSL flags.
- Security integration chain (`security_integration` LDAP) continues to work; cache is a transparent optimization that should honor the same configuration and failure semantics.
- LDAP user groups (`group_provider`) stay managed by `LDAPGroupProvider`; no group/role data in the user cache. Auth continues to set `distinguishedName` for group lookups.

## Detailed Work Plan
1) **Config & defaults**: add FE config keys and validation; default `enable=false`, `ttl=300s`, `max_entries=1000`, `store_password=false`, `negative_ttl=60s`.
2) **Data model**: implement `LdapUserCacheEntry` (immutable) with helpers: `isExpired(now)`, `matchesPassword(password)` when hashed is stored, `isNegative()`, `withPassword(hash)`, `lastBindTime` getter.
3) **Cache manager**: create `LdapUserCache` helper with `get`, `putSuccess`, `putNegative`, `invalidate`, `stats`; enforce size cap & TTL; consider Guava Cache if available, else LinkedHashMap with synchronized block or Caffeine if dependency exists. No overlap with `LDAPGroupProvider` cache (group membership remains there).
4) **Hook into auth**: update `LDAPAuthProvider.authenticate` to consult cache before bind; on hit: if password policy satisfied, skip bind; otherwise re-bind and refresh entry. On miss: resolve DN, bind, cache success/negative accordingly.
5) **DN resolution caching**: cache successful DN lookups even when password not stored; use TTL; still re-bind per password unless password caching enabled. Do not mix in group/role data—`LDAPGroupProvider` continues to own group membership caching.
6) **Security review**: ensure passwords are not logged; hash in-memory; clear cache on config reload or FE restart; allow admin flush via `AuthenticationMgr` or a new SQL admin statement if desired.
7) **Metrics/observability**: add FE metric counters (hit, miss, negative hit, evict) and optional debug logs guarded by LOG.isDebugEnabled.
8) **Tests**: add unit tests for cache entry expiry, negative caching, password match, size eviction; integration tests mocking LDAP to assert reduced bind calls and correct DN propagation; regression for wrong password after cache.
9) **Docs**: update `docs/en/administration/user_privs/authentication/ldap_authentication.md`, `user_privs/authentication/security_integration.md`, and `user_privs/group_provider.md` (and zh) with cache settings, cautions about password storage, and defaults; explicitly state user cache leaves group caching to `LDAPGroupProvider`.
10) **Upgrade behavior**: keep cache off by default; ensure persistence not required (cache is ephemeral); handle missing configs gracefully.

## Double-Check Checklist (correctness & safety)
- TTL enforced on every access (no stale auth beyond config window).
- Size cap prevents unbounded memory growth; eviction policy defined and tested.
- Wrong password after a cached success triggers re-bind failure (no false accepts).
- Negative cache expires quickly; does not hide newly created LDAP users for long.
- DN is always set on context from cached/resolved value.
- Passwords never logged; hash in-memory only when explicitly enabled.
- Cache cleared on FE restart or manual invalidate path; config reload respected.
- Concurrent logins do not race to corrupt cache (thread-safe structures tested).

## TODO (with checkboxes)
- [x] Add FE configs for LDAP user cache (enable, TTLs, max entries, password handling) with validation and sane defaults.
- [x] Implement `LdapUserCacheEntry` (immutable) with expiry and optional password hash/token support, plus negative-entry support.
- [x] Implement `LdapUserCache` manager (bounded, concurrent, TTL-aware) with hit/miss/evict stats and invalidation.
- [x] Integrate cache into `LDAPAuthProvider.authenticate` (DN lookup caching, bind reuse policy, negative caching) and wire context DN setting.
- [x] Add metrics/logging hooks for cache behavior and expose via FE metrics.
- [x] Write unit/integration tests covering cache hits/misses, expiry, negative cache, size cap eviction, and wrong-password scenarios.
- [x] Update authentication docs (en/zh) describing LDAP user cache configs, defaults, and security considerations.
- [ ] (Optional) Add admin command/API to flush LDAP user cache for emergency invalidation.

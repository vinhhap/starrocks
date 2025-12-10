// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.authentication;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.starrocks.common.Config;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.MetricRepo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple bounded cache for LDAP user lookups and bind results.
 */
public class LdapUserCache {
    private static final Logger LOG = LogManager.getLogger(LdapUserCache.class);

    private final Cache<LdapUserCacheKey, LdapUserCacheEntry> cache;
    private final LdapUserCacheConfig config;
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong negativeHits = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();

    public LdapUserCache(LdapUserCacheConfig config) {
        this.config = config;
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(config.maxEntries())
                .removalListener(this::onRemoval)
                .build();
    }

    private void onRemoval(RemovalNotification<LdapUserCacheKey, LdapUserCacheEntry> notification) {
        if (notification.wasEvicted()) {
            evictions.incrementAndGet();
            increaseCounter(MetricRepo.COUNTER_LDAP_USER_CACHE_EVICTION);
        }
    }

    public Optional<LdapUserCacheEntry> getIfValid(LdapUserCacheKey key, long nowMillis) {
        LdapUserCacheEntry entry = cache.getIfPresent(key);
        if (entry == null) {
            misses.incrementAndGet();
            increaseCounter(MetricRepo.COUNTER_LDAP_USER_CACHE_MISS);
            return Optional.empty();
        }

        if (entry.isExpired(nowMillis)) {
            cache.invalidate(key);
            misses.incrementAndGet();
            increaseCounter(MetricRepo.COUNTER_LDAP_USER_CACHE_MISS);
            return Optional.empty();
        }

        if (entry.isNegative()) {
            negativeHits.incrementAndGet();
            increaseCounter(MetricRepo.COUNTER_LDAP_USER_CACHE_NEGATIVE_HIT);
        } else {
            hits.incrementAndGet();
            increaseCounter(MetricRepo.COUNTER_LDAP_USER_CACHE_HIT);
        }
        return Optional.of(entry);
    }

    public void putSuccess(LdapUserCacheKey key, String distinguishedName, String password, long nowMillis) {
        if (!config.enable()) {
            return;
        }

        byte[] passwordHash = null;
        if (config.storePassword() && password != null) {
            passwordHash = hashPassword(password);
        }
        long expireAt = nowMillis + config.ttlMillis();
        cache.put(key, LdapUserCacheEntry.success(key.username(), distinguishedName, passwordHash, expireAt, nowMillis));
    }

    public void putNegative(LdapUserCacheKey key, long nowMillis) {
        if (!config.enable()) {
            return;
        }
        long expireAt = nowMillis + config.negativeTtlMillis();
        cache.put(key, LdapUserCacheEntry.negative(key.username(), expireAt));
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }

    public LdapUserCacheStats snapshot() {
        return new LdapUserCacheStats(hits.get(), misses.get(), negativeHits.get(), evictions.get(), cache.size());
    }

    public LdapUserCacheConfig getConfig() {
        return config;
    }

    private void increaseCounter(LongCounterMetric metric) {
        if (metric != null) {
            metric.increase(1L);
        }
    }

    private static byte[] hashPassword(String password) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(password.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("missing SHA-256", e);
        }
    }

    public static LdapUserCacheInstance getOrCreateFromConfig() {
        LdapUserCacheConfig current = LdapUserCacheConfig.fromConfig();
        if (!current.enable()) {
            LdapUserCacheHolder.STATE.set(null);
            return new LdapUserCacheInstance(current, null);
        }

        LdapUserCacheState state = LdapUserCacheHolder.STATE.get();
        if (state == null || !state.config.equals(current)) {
            LdapUserCache cache = new LdapUserCache(current);
            state = new LdapUserCacheState(current, cache);
            LdapUserCacheHolder.STATE.set(state);
            LOG.info("Initialized LDAP user cache with ttl={}s negative_ttl={}s max_entries={} store_password={}",
                    current.ttlSeconds(), current.negativeTtlSeconds(), current.maxEntries(), current.storePassword());
        }
        return new LdapUserCacheInstance(state.config, state.cache);
    }

    private static class LdapUserCacheHolder {
        private static final java.util.concurrent.atomic.AtomicReference<LdapUserCacheState> STATE =
                new java.util.concurrent.atomic.AtomicReference<>();
    }

    private static class LdapUserCacheState {
        private final LdapUserCacheConfig config;
        private final LdapUserCache cache;

        private LdapUserCacheState(LdapUserCacheConfig config, LdapUserCache cache) {
            this.config = config;
            this.cache = cache;
        }
    }

    public record LdapUserCacheInstance(LdapUserCacheConfig config, LdapUserCache cache) {
        public boolean enabled() {
            return config.enable() && cache != null;
        }
    }

    public record LdapUserCacheStats(long hits, long misses, long negativeHits, long evictions, long size) {
    }

    public record LdapUserCacheConfig(boolean enable,
                                      long ttlMillis,
                                      long negativeTtlMillis,
                                      int maxEntries,
                                      boolean storePassword) {
        public static LdapUserCacheConfig fromConfig() {
            long ttlSeconds = Math.max(0, Config.authentication_ldap_simple_user_cache_ttl_seconds);
            long negativeTtlSeconds = Math.max(0, Config.authentication_ldap_simple_user_cache_negative_ttl_seconds);
            int maxEntries = Math.max(1, Config.authentication_ldap_simple_user_cache_max_entries);
            return new LdapUserCacheConfig(Config.authentication_ldap_simple_user_cache_enable,
                    ttlSeconds * 1000,
                    negativeTtlSeconds * 1000,
                    maxEntries,
                    Config.authentication_ldap_simple_user_cache_store_password);
        }

        public long ttlSeconds() {
            return ttlMillis / 1000;
        }

        public long negativeTtlSeconds() {
            return negativeTtlMillis / 1000;
        }
    }

    public record LdapUserCacheEntry(String username,
                                     String distinguishedName,
                                     boolean exists,
                                     byte[] passwordHash,
                                     long expireAtMillis,
                                     long lastBindTimeMillis) {
        public boolean isNegative() {
            return !exists;
        }

        public boolean isExpired(long nowMillis) {
            return nowMillis >= expireAtMillis;
        }

        public boolean hasPasswordHash() {
            return passwordHash != null && passwordHash.length > 0;
        }

        public boolean matchesPassword(String password) {
            if (!hasPasswordHash() || password == null) {
                return false;
            }
            byte[] candidate = hashPassword(password);
            return MessageDigest.isEqual(passwordHash, candidate);
        }

        public static LdapUserCacheEntry success(String username, String distinguishedName, byte[] passwordHash,
                                                 long expireAtMillis, long lastBindTimeMillis) {
            return new LdapUserCacheEntry(username, distinguishedName, true, passwordHash, expireAtMillis,
                    lastBindTimeMillis);
        }

        public static LdapUserCacheEntry negative(String username, long expireAtMillis) {
            return new LdapUserCacheEntry(username, "", false, null, expireAtMillis, 0L);
        }
    }

    public record LdapUserCacheKey(String username,
                                   String ldapServerHost,
                                   int ldapServerPort,
                                   boolean useSSL,
                                   String ldapBindBaseDN,
                                   String ldapSearchFilter,
                                   String ldapUserDN) {
        public LdapUserCacheKey {
            username = username == null ? "" : username.toLowerCase();
            ldapServerHost = ldapServerHost == null ? "" : ldapServerHost.toLowerCase();
            ldapBindBaseDN = normalize(ldapBindBaseDN);
            ldapSearchFilter = normalize(ldapSearchFilter);
            ldapUserDN = normalize(ldapUserDN);
        }

        private static String normalize(String value) {
            return value == null ? "" : value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LdapUserCacheKey)) {
                return false;
            }
            LdapUserCacheKey that = (LdapUserCacheKey) o;
            return ldapServerPort == that.ldapServerPort && useSSL == that.useSSL
                    && Objects.equals(username, that.username)
                    && Objects.equals(ldapServerHost, that.ldapServerHost)
                    && Objects.equals(ldapBindBaseDN, that.ldapBindBaseDN)
                    && Objects.equals(ldapSearchFilter, that.ldapSearchFilter)
                    && Objects.equals(ldapUserDN, that.ldapUserDN);
        }

        @Override
        public int hashCode() {
            return Objects.hash(username, ldapServerHost, ldapServerPort, useSSL, ldapBindBaseDN,
                    ldapSearchFilter, ldapUserDN);
        }
    }
}

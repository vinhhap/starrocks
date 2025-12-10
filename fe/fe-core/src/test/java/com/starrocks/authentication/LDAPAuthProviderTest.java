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

import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.LdapUserCache.LdapUserCacheInstance;
import com.starrocks.common.Config;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.NodePosition;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

class LDAPAuthProviderTest {

    private static final AtomicInteger BIND_CALLS = new AtomicInteger();
    private static final AtomicInteger LOOKUP_CALLS = new AtomicInteger();

    @BeforeAll
    public static void setUp() throws Exception {
        BIND_CALLS.set(0);
        LOOKUP_CALLS.set(0);
        new MockUp<LDAPAuthProvider>() {
            @Mock
            private void checkPassword(String dn, String password) throws Exception {
                BIND_CALLS.incrementAndGet();
            }

            @Mock
            private String findUserDNByRoot(String user) throws Exception {
                LOOKUP_CALLS.incrementAndGet();
                if ("unknown".equals(user)) {
                    throw new LdapUserNotFoundException("not found");
                }
                return "uid=test,ou=People,dc=starrocks,dc=com";
            }
        };

        // Mock EditLog
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        authenticationMgr.createUser(new CreateUserStmt(
                new UserIdentity("ldap_user", "%"),
                true,
                new UserAuthOption("AUTHENTICATION_LDAP_SIMPLE",
                        "uid=ldap_user,ou=company,dc=example,dc=com",
                        false, NodePosition.ZERO),
                List.of(), Map.of(), NodePosition.ZERO));

        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "file");
        properties.put(FileGroupProvider.GROUP_FILE_URL, "file_group");

        String groupName = "file_group_provider";
        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.authentication_ldap_simple_user_cache_enable = false;
        Config.authentication_ldap_simple_user_cache_store_password = false;
        Config.authentication_ldap_simple_user_cache_ttl_seconds = 300;
        Config.authentication_ldap_simple_user_cache_negative_ttl_seconds = 60;
        Config.authentication_ldap_simple_user_cache_max_entries = 1000;
    }

    @AfterAll
    public static void teardown() {
        Config.authentication_ldap_simple_user_cache_enable = false;
        Config.authentication_ldap_simple_user_cache_store_password = false;
        Config.authentication_ldap_simple_user_cache_ttl_seconds = 300;
        Config.authentication_ldap_simple_user_cache_negative_ttl_seconds = 60;
        Config.authentication_ldap_simple_user_cache_max_entries = 1000;
        BIND_CALLS.set(0);
        LOOKUP_CALLS.set(0);
    }

    private void enableCache(boolean storePassword) {
        Config.authentication_ldap_simple_user_cache_enable = true;
        Config.authentication_ldap_simple_user_cache_store_password = storePassword;
        Config.authentication_ldap_simple_user_cache_ttl_seconds = 300;
        Config.authentication_ldap_simple_user_cache_negative_ttl_seconds = 60;
        Config.authentication_ldap_simple_user_cache_max_entries = 1000;
        LdapUserCacheInstance instance = LdapUserCache.getOrCreateFromConfig();
        if (instance.cache() != null) {
            instance.cache().invalidateAll();
        }
        BIND_CALLS.set(0);
        LOOKUP_CALLS.set(0);
    }

    @Test
    void testAuthenticateSetsDNWhenLdapUserDNProvided() throws Exception {

        String providedDN = "cn=test,ou=People,dc=starrocks,dc=com";
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                "cn=admin,dc=starrocks,dc=com", "secret",
                "ou=People,dc=starrocks,dc=com", "uid",
                providedDN);

        ConnectContext context = new ConnectContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("ldap_user", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        provider.authenticate(context, user, authResponse);
        Assertions.assertEquals(providedDN, context.getDistinguishedName());
    }

    @Test
    void testAuthenticateSetsDNWhenFindByRoot() throws Exception {
        enableCache(false);
        String discoveredDN = "uid=test,ou=People,dc=starrocks,dc=com";
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                "cn=admin,dc=starrocks,dc=com", "secret",
                "ou=People,dc=starrocks,dc=com", "uid",
                /* ldapUserDN */ null
        );

        ConnectContext context = new ConnectContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("ldap_user", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        provider.authenticate(context, user, authResponse);
        Assertions.assertEquals(discoveredDN, context.getDistinguishedName());
    }

    @Test
    void testCacheHitSkipsBindWhenPasswordMatches() throws Exception {
        enableCache(true);
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                "cn=admin,dc=starrocks,dc=com", "secret",
                "ou=People,dc=starrocks,dc=com", "uid",
                "cn=cached,ou=People,dc=starrocks,dc=com");

        ConnectContext context = new ConnectContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("ldap_user", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        provider.authenticate(context, user, authResponse);
        Assertions.assertEquals(1, BIND_CALLS.get());

        provider.authenticate(context, user, authResponse);
        Assertions.assertEquals(1, BIND_CALLS.get(), "cached password should skip bind");
    }

    @Test
    void testPasswordChangeTriggersRebind() throws Exception {
        enableCache(true);
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                "cn=admin,dc=starrocks,dc=com", "secret",
                "ou=People,dc=starrocks,dc=com", "uid",
                "cn=rebinding,ou=People,dc=starrocks,dc=com");

        ConnectContext context = new ConnectContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("ldap_user", "%");

        provider.authenticate(context, user, "password1\0".getBytes(StandardCharsets.UTF_8));
        provider.authenticate(context, user, "password2\0".getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals(2, BIND_CALLS.get(), "password change should trigger rebind");

        provider.authenticate(context, user, "password2\0".getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals(2, BIND_CALLS.get(), "cached new password should skip bind");
    }

    @Test
    void testNegativeCacheSkipsLookup() {
        enableCache(false);
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                "cn=admin,dc=starrocks,dc=com", "secret",
                "ou=People,dc=starrocks,dc=com", "uid",
                null);

        ConnectContext context = new ConnectContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("unknown", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        Assertions.assertThrows(AuthenticationException.class,
                () -> provider.authenticate(context, user, authResponse));
        Assertions.assertEquals(1, LOOKUP_CALLS.get());

        Assertions.assertThrows(AuthenticationException.class,
                () -> provider.authenticate(context, user, authResponse));
        Assertions.assertEquals(1, LOOKUP_CALLS.get(), "negative cache should avoid repeated lookups");
        Assertions.assertEquals(0, BIND_CALLS.get());
    }
}

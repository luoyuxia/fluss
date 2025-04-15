/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.rpc.netty.authenticate;

import com.alibaba.fluss.config.ConfigOption;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.AuthenticationException;
import com.alibaba.fluss.security.auth.ClientAuthenticationPlugin;
import com.alibaba.fluss.security.auth.ClientAuthenticator;
import com.alibaba.fluss.security.auth.ServerAuthenticationPlugin;
import com.alibaba.fluss.security.auth.ServerAuthenticator;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.alibaba.fluss.config.ConfigBuilder.key;

/** A test authentication plugin which need username and password. */
public class UsernamePasswordAuthenticationPlugin
        implements ServerAuthenticationPlugin, ClientAuthenticationPlugin {

    private static final ConfigOption<String> USERNAME =
            key("username").stringType().noDefaultValue();

    private static final ConfigOption<String> PASSWORD =
            key("password").stringType().noDefaultValue();

    private static final String AUTH_PROTOCOL = "username_password";

    @Override
    public String authProtocol() {
        return AUTH_PROTOCOL;
    }

    @Override
    public ClientAuthenticator createClientAuthenticator(Configuration configuration) {
        String username = configuration.getString(USERNAME);
        String password = configuration.getString(PASSWORD);
        if (username == null || password == null) {
            throw new AuthenticationException("username and password shouldn't be null.");
        }
        return new ClientAuthenticator() {
            volatile boolean isComplete = false;

            @Override
            public String protocol() {
                return AUTH_PROTOCOL;
            }

            @Override
            public byte[] authenticate(byte[] data) {
                isComplete = true;
                return serializeToken(username, password);
            }

            @Override
            public boolean isComplete() {
                return isComplete;
            }
        };
    }

    @Override
    public ServerAuthenticator createServerAuthenticator(Configuration configuration) {
        String expectedUsername = configuration.getString(USERNAME);
        String expectedPassword = configuration.getString(PASSWORD);
        if (expectedPassword == null || expectedUsername == null) {
            throw new AuthenticationException("username and password shouldn't be null.");
        }
        return new ServerAuthenticator() {
            volatile boolean isComplete = false;

            @Override
            public String protocol() {
                return AUTH_PROTOCOL;
            }

            @Override
            public byte[] evaluateResponse(byte[] token) {
                byte[] expectedToken = serializeToken(expectedUsername, expectedPassword);
                if (!Arrays.equals(token, expectedToken)) {
                    throw new AuthenticationException("username or password is incorrect.");
                }

                isComplete = true;

                return new byte[0];
            }

            @Override
            public boolean isComplete() {
                return isComplete;
            }
        };
    }

    private byte[] serializeToken(String username, String password) {
        return (username + "," + password).getBytes(StandardCharsets.UTF_8);
    }
}

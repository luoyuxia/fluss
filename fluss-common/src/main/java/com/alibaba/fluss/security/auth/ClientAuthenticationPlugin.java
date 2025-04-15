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

package com.alibaba.fluss.security.auth;

import com.alibaba.fluss.config.Configuration;

/**
 * Defines the client-side authentication plugin interface for implementing protocol-specific
 * identity validation logic.
 *
 * <p>The protocol type is specified via {@link
 * com.alibaba.fluss.config.ConfigOptions#CLIENT_SECURITY_PROTOCOL}, and configuration parameters
 * must be prefixed with {@code client.security.${protocol}}.
 */
public interface ClientAuthenticationPlugin extends AuthenticationPlugin {
    /**
     * Creates a client-side authenticator instance for this authentication protocol.
     *
     * @return A new client authenticator instance implementing the protocol defined by {@link
     *     #authProtocol()}
     */
    ClientAuthenticator createClientAuthenticator(Configuration configuration);
}

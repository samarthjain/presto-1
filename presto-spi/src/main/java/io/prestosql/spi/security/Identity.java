/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.spi.security;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class Identity
{
    private final String user;
    private final Optional<Principal> principal;
    private final Map<String, SelectedRole> roles;
    private final Map<String, String> extraCredentials;
    private Map<String, Map<String, String>> sessionPropertiesByCatalog = new HashMap<>();

    public Identity(String user, Optional<Principal> principal)
    {
        this(user, principal, emptyMap());
    }

    public Identity(String user, Optional<Principal> principal, Map<String, SelectedRole> roles)
    {
        this(user, principal, roles, emptyMap());
    }

    public Identity(String user, Optional<Principal> principal, Map<String, SelectedRole> roles, Map<String, String> extraCredentials)
    {
        this(user, principal, roles, extraCredentials, emptyMap());
    }

    public Identity(String user, Optional<Principal> principal, Map<String, SelectedRole> roles, Map<String, String> extraCredentials, Map<String, Map<String, String>> sessionPropertiesByCatalog)
    {
        this.user = requireNonNull(user, "user is null");
        this.principal = requireNonNull(principal, "principal is null");
        this.roles = unmodifiableMap(requireNonNull(roles, "roles is null"));
        this.extraCredentials = unmodifiableMap(new HashMap<>(requireNonNull(extraCredentials, "extraCredentials is null")));
        this.sessionPropertiesByCatalog = sessionPropertiesByCatalog;
    }

    public String getUser()
    {
        return user;
    }

    public Optional<Principal> getPrincipal()
    {
        return principal;
    }

    public Map<String, SelectedRole> getRoles()
    {
        return roles;
    }

    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials;
    }

    public ConnectorIdentity toConnectorIdentity()
    {
        return new ConnectorIdentity(user, principal, Optional.empty(), extraCredentials, emptyMap());
    }

    public ConnectorIdentity toConnectorIdentity(String catalog)
    {
        requireNonNull(catalog, "catalog is null");

        // The father all of hecks. Users no longer want to specify the same role configs for each catalogs i.e. hive,iceberg,testhive
        // and so on, so we just union all catalogs aws_iam_role session properties and pass that down.
        Map<String, String> mergedSessionProperties = new HashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : this.sessionPropertiesByCatalog.entrySet()) {
            for (Map.Entry<String, String> keyVal : entry.getValue().entrySet()) {
                if (keyVal.getKey().startsWith("aws_iam_role")) {
                    mergedSessionProperties.put(keyVal.getKey(), keyVal.getValue());
                }
            }
        }

        if (this.sessionPropertiesByCatalog.get(catalog) != null) {
            mergedSessionProperties.putAll(this.sessionPropertiesByCatalog.get(catalog));
        }

        return new ConnectorIdentity(user, principal, Optional.ofNullable(roles.get(catalog)), extraCredentials, mergedSessionProperties);
    }

    public void setSessionPropertiesByCatalog(Map<String, Map<String, String>> sessionPropertiesByCatalog)
    {
        this.sessionPropertiesByCatalog = sessionPropertiesByCatalog;
    }

    public Map<String, Map<String, String>> getSessionPropertiesByCatalog()
    {
        return sessionPropertiesByCatalog;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Identity identity = (Identity) o;
        return Objects.equals(user, identity.user);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(user);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("Identity{");
        sb.append("user='").append(user).append('\'');
        principal.ifPresent(principal -> sb.append(", principal=").append(principal));
        sb.append(", roles=").append(roles);
        sb.append(", extraCredentials=").append(extraCredentials.keySet());
        sb.append('}');
        return sb.toString();
    }
}

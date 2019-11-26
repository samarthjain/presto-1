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
package io.prestosql.plugin.hive.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.type.TypeSignature;

import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class ConnectorCommonViewDefinition
{
    private final String originalSql;
    private final String sessionCatalog;
    private final List<String> sessionNamespace;
    private final org.apache.iceberg.Schema columns;
    private final Optional<String> owner;

    @JsonCreator
    public ConnectorCommonViewDefinition(
            @JsonProperty("originalSql") String originalSql,
            @JsonProperty("sessionCatalog") String sessionCatalog,
            @JsonProperty("sessionNamespace") List<String> sessionNamespace,
            @JsonProperty("columns") org.apache.iceberg.Schema columns,
            @JsonProperty("owner") Optional<String> owner)
    {
        this.originalSql = requireNonNull(originalSql, "originalSql is null");
        this.sessionCatalog = sessionCatalog;
        this.sessionNamespace = sessionNamespace;
        this.columns = requireNonNull(columns, "columns are null");

        this.owner = owner;
        if (columns.columns().isEmpty()) {
            throw new IllegalArgumentException("columns list is empty");
        }
    }

    @JsonProperty
    public String getOriginalSql()
    {
        return originalSql;
    }

    @JsonProperty
    public String getCatalog()
    {
        return sessionCatalog;
    }

    @JsonProperty
    public List<String> getSchema()
    {
        return sessionNamespace;
    }

    @JsonProperty
    public org.apache.iceberg.Schema getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<String> getOwner()
    {
        return owner;
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        owner.ifPresent(value -> joiner.add("owner=" + value));
        joiner.add("columns=" + columns);
        joiner.add("catalog=" + sessionCatalog);
        joiner.add("schema=" + getSchema());
        joiner.add("originalSql=[" + originalSql + "]");
        return getClass().getSimpleName() + joiner.toString();
    }

    public static final class ViewColumn
    {
        private final String name;
        private final TypeSignature type;
        private final String alias;
        private final String comment;

        @JsonCreator
        public ViewColumn(
                @JsonProperty("name") String name,
                @JsonProperty("type") TypeSignature type,
                @JsonProperty("alias") String alias,
                @JsonProperty("comment") String comment)
        {
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
            this.alias = alias;
            this.comment = comment;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public TypeSignature getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return name + " " + type + " " + alias + " " + comment;
        }
    }
}

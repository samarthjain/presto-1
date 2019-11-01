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
package io.prestosql.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.type.TypeSignature;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class ConnectorViewDefinition
{
    private final String originalSql;

    private final Optional<String> viewExpandedText;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final List<ViewColumn> columns;
    private final Optional<String> owner;
    private final boolean runAsInvoker;
    private final boolean isPrestoView;

    @JsonCreator
    public ConnectorViewDefinition(
            @JsonProperty("originalSql") String originalSql,
            @JsonProperty("viewExpandedText") Optional<String> viewExpandedText,
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("columns") List<ViewColumn> columns,
            @JsonProperty("owner") Optional<String> owner,
            @JsonProperty("runAsInvoker") boolean runAsInvoker,
            @JsonProperty("isPrestoView") boolean isPrestoView)
    {
        this.originalSql = requireNonNull(originalSql, "originalSql is null");
        this.viewExpandedText = viewExpandedText;
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.columns = unmodifiableList(new ArrayList<>(requireNonNull(columns, "columns is null")));
        this.owner = requireNonNull(owner, "owner is null");
        this.runAsInvoker = runAsInvoker;
        this.isPrestoView = isPrestoView;
        if (isPrestoView && !catalog.isPresent() && schema.isPresent()) {
            throw new IllegalArgumentException("catalog must be present if schema is present");
        }
        if (runAsInvoker && owner.isPresent()) {
            throw new IllegalArgumentException("owner cannot be present with runAsInvoker");
        }
        if (isPrestoView && columns.isEmpty()) {
            throw new IllegalArgumentException("columns list is empty");
        }
    }

    // constructor for hive views
    public ConnectorViewDefinition(
            String originalSql,
            Optional<String> viewExpandedText,
            Optional<String> schema,
            Optional<String> owner)
    {
        this(originalSql, viewExpandedText, Optional.empty(), schema, Collections.emptyList(), owner, false, false);
    }

    // constructor for presto views
    public ConnectorViewDefinition(
            String originalSql,
            Optional<String> catalog,
            Optional<String> schema,
            List<ViewColumn> columns,
            Optional<String> owner,
            boolean runAsInvoker)
    {
        this(originalSql, Optional.empty(), catalog, schema, columns, owner, runAsInvoker, true);
    }

    @JsonProperty
    public String getOriginalSql()
    {
        return originalSql;
    }

    @JsonProperty
    public Optional<String> getViewExpandedText()
    {
        return viewExpandedText;
    }

    @JsonProperty
    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public Optional<String> getSchema()
    {
        return schema;
    }

    @JsonProperty
    public List<ViewColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<String> getOwner()
    {
        return owner;
    }

    @JsonProperty
    public boolean isRunAsInvoker()
    {
        return runAsInvoker;
    }

    @JsonProperty
    public boolean isPrestoView()
    {
        return isPrestoView;
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        owner.ifPresent(value -> joiner.add("owner=" + value));
        joiner.add("runAsInvoker=" + runAsInvoker);
        joiner.add("columns=" + columns);
        catalog.ifPresent(value -> joiner.add("catalog=" + value));
        schema.ifPresent(value -> joiner.add("schema=" + value));
        joiner.add("originalSql=[" + originalSql + "]");
        return getClass().getSimpleName() + joiner.toString();
    }

    public static final class ViewColumn
    {
        private final String name;
        private final TypeSignature type;

        @JsonCreator
        public ViewColumn(
                @JsonProperty("name") String name,
                @JsonProperty("type") TypeSignature type)
        {
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
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
            return name + " " + type;
        }
    }
}

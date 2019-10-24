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
package io.prestosql.plugin.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.prestosql.plugin.iceberg.TableType.DATA;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IcebergTableHandle
        implements ConnectorTableHandle
{
    private static final Pattern TABLE_PATTERN = Pattern.compile("(?<table>(?:[^$@_]|_[^$@_])+)" +
            "(?:(?:@|__)(?<ver1>\\d+))?" +
            "(?:(?:\\$|__)(?<type>(?:history|snapshots|manifests|partitions))(?:(?:@|__)(?<ver2>\\d+))?)?");

    private final String schemaName;
    private final String tableName;
    private final TableType tableType;
    private final Optional<Long> snapshotId;
    private final TupleDomain<HiveColumnHandle> predicate;

    @JsonCreator
    public IcebergTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableType") TableType tableType,
            @JsonProperty("snapshotId") Optional<Long> snapshotId,
            @JsonProperty("predicate") TupleDomain<HiveColumnHandle> predicate)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TableType getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public Optional<Long> getSnapshotId()
    {
        return snapshotId;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getPredicate()
    {
        return predicate;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return getSchemaTableName().toString();
    }

    public static IcebergTableHandle from(SchemaTableName name)
    {
        Matcher match = TABLE_PATTERN.matcher(name.getTableName());
        if (match.matches()) {
            String table = match.group("table");
            String typeStr = match.group("type");
            String ver1 = match.group("ver1");
            String ver2 = match.group("ver2");

            TableType type = DATA;
            if (typeStr != null) {
                try {
                    type = TableType.valueOf(typeStr.toUpperCase(Locale.ROOT));
                }
                catch (IllegalArgumentException e) {
                    throw new PrestoException(NOT_SUPPORTED, format("Invalid Iceberg table name (unknown type '%s'): %s", typeStr, name));
                }
            }

            Optional<Long> version = Optional.empty();
            if (type == DATA ||
                    type == TableType.PARTITIONS ||
                    type == TableType.MANIFESTS) {
                Preconditions.checkArgument(ver1 == null || ver2 == null,
                        "Cannot specify two versions");
                if (ver1 != null) {
                    version = Optional.ofNullable(parseLong(ver1));
                }
                else if (ver2 != null) {
                    version = Optional.ofNullable(parseLong(ver2));
                }
            }
            else {
                Preconditions.checkArgument(ver1 == null && ver2 == null,
                        "Cannot use version with table type %s: %s", typeStr, name.getTableName());
            }

            return new IcebergTableHandle(name.getSchemaName(), table, type, version, TupleDomain.all());
        }

        return new IcebergTableHandle(name.getSchemaName(), name.getTableName(), DATA, Optional.empty(), TupleDomain.all());
    }
}

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
package io.prestosql.plugin.hive.netflix;

import com.google.common.base.Splitter;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.hive.HiveSessionProperties.AWS_IAM_ROLE;
import static io.prestosql.spi.security.AccessDeniedException.denyDropTable;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NetflixAccessControl
        implements ConnectorAccessControl
{
    private static final Splitter SPLITTER = Splitter.on('=').trimResults().omitEmptyStrings();

    private final HiveMetastore metastore;
    private List<String> s3RoleMappings;

    @Inject
    public NetflixAccessControl(HiveMetastore metastore, HiveConfig hiveConfig)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        requireNonNull(hiveConfig, "hiveConfig is null");
        this.s3RoleMappings = requireNonNull(hiveConfig.getS3RoleMappings(), "s3RoleMappings is null");
    }

    @Override
    public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName)
    {
    }

    @Override
    public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName)
    {
    }

    @Override
    public void checkCanRenameSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName, String newSchemaName)
    {
    }

    @Override
    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity)
    {
    }

    @Override
    public void checkCanSetTableComment(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanShowTablesMetadata(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        Optional<Table> target = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());

        if (!target.isPresent()) {
            denyDropTable(tableName.toString(), "Table not found");
        }
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
    }

    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanDropColumn(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName, Set<String> colunmNames)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName viewName)
    {
        checkAccess(identity, viewName);
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName viewName)
    {
        checkAccess(identity, viewName);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName viewName, Set<String> columnNames)
    {
        checkAccess(identity, viewName);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Privilege privilege, SchemaTableName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Privilege privilege, SchemaTableName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        checkAccess(identity, tableName);
    }

    public void checkCanShowColumnsMetadata(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
        checkAccess(identity, tableName);
    }

    /**
     * Filter the list of columns to those visible to the identity.
     */
    public List<ColumnMetadata> filterColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName, List<ColumnMetadata> columns)
    {
        return columns;
    }

    private void checkAccess(ConnectorIdentity identity, SchemaTableName tableName)
    {
        for (String roleMapping : this.s3RoleMappings) {
            List<String> splitted = SPLITTER.splitToList(roleMapping);
            checkArgument(splitted.size() == 2, "Splitted s3 role mapping should have two elements (e.g., schema=role)");
            String schema = splitted.get(0);
            String roleName = splitted.get(1);
            // the user is accessing a schema that has a configured role mapping
            if (Objects.equals(tableName.getSchemaName(), schema)) {
                Map<String, String> sessionProperties = identity.getSessionProperties();

                if (sessionProperties == null || sessionProperties.isEmpty()) {
                    denyAccess(schema);
                }

                Optional<Map.Entry<String, String>> roleProperty = sessionProperties
                        .entrySet()
                        .stream()
                        .filter(entry -> entry.getKey().equalsIgnoreCase(AWS_IAM_ROLE + "_" + schema)).findAny();

                if (roleProperty.isPresent() == false) {
                    roleProperty = sessionProperties
                            .entrySet()
                            .stream()
                            .filter(entry -> entry.getKey().equalsIgnoreCase(AWS_IAM_ROLE)).findAny();
                }

                if (!roleProperty.isPresent()) {
                    denyAccess(schema);
                }

                String sessionRoleName = roleProperty.get().getValue();
                if (!Objects.equals(roleName, sessionRoleName)) {
                    denyAccess(schema);
                }
            }
        }
    }

    private void denyAccess(String schema)
    {
        throw new AccessDeniedException(format("To access this table you should specify the role arn for %s with the %s session property " +
                        "on the catalog that you are accessing: \"set session ${catalog_name, i.e. prodhive/testhive/iceberg}.%s=<%s role_arn>\"",
                schema, AWS_IAM_ROLE, AWS_IAM_ROLE, schema));
    }
}

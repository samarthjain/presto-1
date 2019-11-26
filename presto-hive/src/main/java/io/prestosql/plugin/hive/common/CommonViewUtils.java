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

import com.google.common.base.Preconditions;
import com.netflix.bdp.view.ViewDefinition;
import com.netflix.iceberg.metacat.MetacatViewCatalog;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.ViewAlreadyExistsException;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveWriteUtils.getTableDefaultLocation;

public class CommonViewUtils
{
    public static final String NETFLIX_METACAT_HOST = "netflix.metacat.host";
    public static final String NETFLIX_WAREHOUSE_DIR = "hive.metastore.warehouse.dir";
    public static final String APP_NAME = "presto-" + System.getenv("stack");

    private static ViewConfig config;

    public CommonViewUtils(ViewConfig viewConfig)
    {
        this.config = viewConfig;
    }

    public MetacatViewCatalog getViewCatalog(Configuration configuration)
    {
        configuration.set(NETFLIX_METACAT_HOST, config.getMetastoreRestEndpoint());
        configuration.set(NETFLIX_WAREHOUSE_DIR, config.getMetastoreWarehouseDir());
        MetacatViewCatalog metacatViewCatalog = new MetacatViewCatalog(configuration, APP_NAME);
        return metacatViewCatalog;
    }

    public static Schema viewColsToIcebergSchema(TypeManager typeManager,
            List<ConnectorViewDefinition.ViewColumn> viewColumns)
    {
        List<Types.NestedField> nestedFields = new ArrayList<>();
        int id = 0;

        for (ConnectorViewDefinition.ViewColumn column : viewColumns) {
            Types.NestedField nestedField = Types.NestedField.optional(id, column.getName(),
                    TypeConverter.toIcebergType(typeManager.getType(column.getType())), "");
            nestedFields.add(nestedField);
            id++;
        }
        return new Schema(nestedFields);
    }

    public static List<ConnectorViewDefinition.ViewColumn> icebergSchemaToViewCols(TypeManager typeManager,
            Schema schema)
    {
        List<ConnectorViewDefinition.ViewColumn> viewColumns = new ArrayList<>();
        for (Types.NestedField field : schema.columns()) {
            TypeSignature type = TypeConverter.toPrestoType(field.type(), typeManager).getTypeSignature();
            ConnectorViewDefinition.ViewColumn viewColumn = new ConnectorViewDefinition.ViewColumn(field.name(), type);
            viewColumns.add(viewColumn);
        }
        return viewColumns;
    }

    public void writeCommonViewDefinition(Configuration configuration,
            Map<String, String> properties,
            ConnectorViewDefinition definition, TypeManager typeManager,
            String catalog, SchemaTableName viewName,
            boolean replace, boolean viewExists)
    {
        org.apache.iceberg.Schema schema = viewColsToIcebergSchema(typeManager, definition.getColumns());
        List<String> namespaces = new ArrayList<>();
        if (definition.getSchema().isPresent()) {
            namespaces.add(definition.getSchema().get());
        }
        ConnectorCommonViewDefinition connectorCommonViewDefinition = new
                ConnectorCommonViewDefinition(definition.getOriginalSql(), definition.getCatalog().get(),
                namespaces,
                schema, definition.getOwner());
        encodeAndWriteCommonViewData(configuration, properties, connectorCommonViewDefinition, catalog, viewName,
                replace, viewExists);
    }

    public void encodeAndWriteCommonViewData(Configuration configuration,
            Map<String, String> properties,
            ConnectorCommonViewDefinition definition,
            String catalogName,
            SchemaTableName name,
            boolean replace, boolean viewExists)
    {
        MetacatViewCatalog catalog = getViewCatalog(configuration);
        Preconditions.checkState(definition.getCatalog() != null);
        ViewDefinition metadata = ViewDefinition.of(definition.getOriginalSql(), definition.getColumns(),
                definition.getCatalog(), definition.getSchema());

        String viewName = catalogName + "." + name.toString();
        if (viewExists) {
            if (!replace) {
                throw new ViewAlreadyExistsException(name);
            }
            catalog.replace(viewName, metadata, properties);
            return;
        }
        catalog.create(viewName, metadata, properties);
    }

    public Optional<ConnectorViewDefinition> decodeCommonViewData(Configuration configuration,
            ConnectorSession session,
            TypeManager typeManager,
            String catalogName,
            SchemaTableName name)
    {
        MetacatViewCatalog catalog = getViewCatalog(configuration);
        String viewName = catalogName + "." + name.toString();
        ViewDefinition viewDefinition = catalog.loadDefinition(viewName);

        String sessionNameSpace = viewDefinition.sessionNamespace().size() > 0 ? viewDefinition.sessionNamespace().get(0) : "";
        ConnectorViewDefinition definition = new ConnectorViewDefinition(viewDefinition.sql(),
                Optional.of(viewDefinition.sessionCatalog()), Optional.of(sessionNameSpace),
                icebergSchemaToViewCols(typeManager, viewDefinition.schema()),
                Optional.empty(), false);

        return Optional.of(definition);
    }

    public void dropView(Configuration configuration, String catalogName, SchemaTableName name) {
        MetacatViewCatalog catalog = getViewCatalog(configuration);
        catalog.drop(catalogName + "." + name.toString());
    }

    public static Path getTargetPath(SchemaTableName name, HdfsEnvironment hdfsEnvironment, Database database,
            ConnectorSession session)
    {
        String schemaName = name.getSchemaName();
        String tableName = name.getTableName();
        HdfsEnvironment.HdfsContext hdfsContext = new HdfsEnvironment.HdfsContext(session, schemaName, tableName);
        return getTableDefaultLocation(database, hdfsContext, hdfsEnvironment, schemaName, tableName);
    }
}

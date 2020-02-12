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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.netflix.bdp.view.CommonViewConstants;
import com.netflix.iceberg.metacat.MetacatIcebergCatalog;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveUtil;
import io.prestosql.plugin.hive.HiveWrittenPartitions;
import io.prestosql.plugin.hive.TableAlreadyExistsException;
import io.prestosql.plugin.hive.ViewAlreadyExistsException;
import io.prestosql.plugin.hive.common.CommonViewUtils;
import io.prestosql.plugin.hive.common.TypeConverter;
import io.prestosql.plugin.hive.common.ViewConfig;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.PrincipalPrivileges;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.iceberg.statistics.IcebergStatisticsProvider;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.connector.ViewNotFoundException;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static io.prestosql.plugin.hive.HiveColumnHandle.pathColumnHandle;
import static io.prestosql.plugin.hive.HiveColumnHandle.updateRowIdHandle;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.prestosql.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.prestosql.plugin.hive.HiveMetadata.PRESTO_VERSION_NAME;
import static io.prestosql.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.prestosql.plugin.hive.HiveSchemaProperties.getLocation;
import static io.prestosql.plugin.hive.HiveSessionProperties.isCommonViewSupportEnabled;
import static io.prestosql.plugin.hive.HiveSessionProperties.isStatisticsEnabled;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.plugin.hive.HiveUtil.PRESTO_VIEW_FLAG;
import static io.prestosql.plugin.hive.HiveUtil.decodeViewData;
import static io.prestosql.plugin.hive.HiveUtil.encodeViewData;
import static io.prestosql.plugin.hive.HiveUtil.isPrestoOrCommonView;
import static io.prestosql.plugin.hive.HiveWriteUtils.getTableDefaultLocation;
import static io.prestosql.plugin.hive.common.TypeConverter.toIcebergType;
import static io.prestosql.plugin.hive.common.TypeConverter.toPrestoType;
import static io.prestosql.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.prestosql.plugin.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static io.prestosql.plugin.iceberg.DomainConverter.convertTupleDomainTypes;
import static io.prestosql.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.prestosql.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.prestosql.plugin.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static io.prestosql.plugin.iceberg.IcebergTableProperties.getFileFormat;
import static io.prestosql.plugin.iceberg.IcebergTableProperties.getPartitioning;
import static io.prestosql.plugin.iceberg.IcebergUtil.getColumns;
import static io.prestosql.plugin.iceberg.IcebergUtil.getDataPath;
import static io.prestosql.plugin.iceberg.IcebergUtil.getFileFormat;
import static io.prestosql.plugin.iceberg.IcebergUtil.isIcebergTable;
import static io.prestosql.plugin.iceberg.IcebergUtil.toTableIdentifier;
import static io.prestosql.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.prestosql.plugin.iceberg.PartitionFields.toPartitionFields;
import static io.prestosql.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.prestosql.spi.security.PrincipalType.USER;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class IcebergMetadata
        implements ConnectorMetadata
{
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final String prestoVersion;
    private final ViewConfig viewConfig;
    private final CommonViewUtils commonViewUtils;

    private IcebergConfig icebergConfig;
    private IcebergUtil icebergUtil;
    private Transaction transaction;
    private final IcebergStatisticsProvider icebergStatisticsProvider;

    @Inject
    public IcebergMetadata(
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec,
            IcebergConfig icebergConfig,
            IcebergUtil icebergUtil,
            String prestoVersion,
            ViewConfig viewConfig,
            IcebergStatisticsProvider icebergStatisticsProvider)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.prestoVersion = requireNonNull(prestoVersion, "prestoVersion is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.icebergConfig = icebergConfig;
        this.icebergUtil = icebergUtil;
        this.viewConfig = viewConfig;
        this.commonViewUtils = new CommonViewUtils(viewConfig);
        this.icebergStatisticsProvider = icebergStatisticsProvider;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases();
    }

    @Override
    public IcebergTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        IcebergTableHandle handle;
        if (table.isPresent()) {
            handle = IcebergTableHandle.from(tableName.getSchemaName(), tableName.getTableName());
        }
        else {
            handle = IcebergTableHandle.from(tableName);
            table = metastore.getTable(handle.getSchemaName(), handle.getTableName());
            if (!table.isPresent()) {
                return null;
            }
        }

        if (!isIcebergTable(table.get()) && !isPrestoOrCommonView(table.get())) {
            throw new UnknownTableTypeException(tableName);
        }
        return handle;
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableHandle table = IcebergTableHandle.from(tableName);
        if (table.getTableType() == TableType.PARTITIONS) {
            Configuration configuration = getConfiguration(session, tableName.getSchemaName());
            org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(table.getSchemaName(), table.getTableName(), configuration, metastore);
            return Optional.of(new PartitionTable(table, session, typeManager, icebergTable));
        }
        return Optional.empty();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        TupleDomain<HiveColumnHandle> predicate = convertTupleDomainTypes(constraint.getSummary().transform(HiveColumnHandle.class::cast));
        IcebergTableHandle tableHandle = (IcebergTableHandle) handle;

        // TODO: this will break delete when partitions have evolved. To actually make it work we have to support row level delete interfaces
        // or change presto's SPI.
        TupleDomain<ColumnHandle> nonPartitionPredicate = constraint.getSummary().getDomains()
                .map(m -> m.entrySet().stream().filter(e -> ((HiveColumnHandle) e.getKey()).getColumnType() != HiveColumnHandle.ColumnType.PARTITION_KEY)
                        .collect(Collectors.toMap((x) -> x.getKey(), Map.Entry::getValue)))
                .map(m -> TupleDomain.withColumnDomains(m)).orElse(TupleDomain.none());

        IcebergTableHandle handleWithPredicate = new IcebergTableHandle(tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.getTableType(),
                tableHandle.getSnapshotId(),
                predicate);
        return Optional.of(new ConstraintApplicationResult<>(handleWithPredicate, nonPartitionPredicate));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return getTableMetadata(session, ((IcebergTableHandle) table).getSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        // TODO: this should skip non-Iceberg tables
        return schemaName.map(Collections::singletonList)
                .orElseGet(metastore::getAllDatabases)
                .stream()
                .flatMap(schema -> metastore.getAllTables(schema).stream()
                        .map(table -> new SchemaTableName(schema, table))
                        .collect(toList())
                        .stream())
                .collect(toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Configuration configuration = getConfiguration(session, table.getSchemaName());
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();
        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(table.getSchemaName(), table.getTableName(), configuration, metastore);
        columns.addAll(getColumns(icebergTable.schema(), icebergTable.spec(), typeManager));
        // add hidden path column
        columns.add(pathColumnHandle());
        return columns.build().stream().collect(toMap(HiveColumnHandle::getName, identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        HiveColumnHandle column = (HiveColumnHandle) columnHandle;
        return new ColumnMetadata(column.getName(), typeManager.getType(column.getTypeSignature()));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = prefix.getTable()
                .map(ignored -> singletonList(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName table : tables) {
            try {
                columns.put(table, getTableMetadata(session, table).getColumns());
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
            catch (UnknownTableTypeException e) {
                // ignore table of unknown type
            }
        }
        return columns.build();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        Optional<String> location = getLocation(properties).map(uri -> {
            try {
                hdfsEnvironment.getFileSystem(new HdfsContext(session, schemaName), new Path(uri));
            }
            catch (IOException | IllegalArgumentException e) {
                throw new PrestoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + uri, e);
            }
            return uri;
        });

        Database database = Database.builder()
                .setDatabaseName(schemaName)
                .setLocation(location)
                .setOwnerType(USER)
                .setOwnerName(session.getUser())
                .build();

        metastore.createDatabase(database);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        // basic sanity check to provide a better error message
        if (!listTables(session, Optional.of(schemaName)).isEmpty() ||
                !listViews(session, Optional.of(schemaName)).isEmpty()) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }
        metastore.dropDatabase(schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        metastore.renameDatabase(source, target);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        Optional<ConnectorNewTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout), ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Schema schema = new Schema(toIceberg(tableMetadata.getColumns()));
//        TODO: if we use this reassigned schema we run into issues for partitioned tables.
//        so for now we are letting go of atomicity and just creating the table.
//        AtomicInteger lastColumnId = new AtomicInteger(0);
//        Schema freshSchema = TypeUtil.assignFreshIds(schema, lastColumnId::incrementAndGet);
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));

        Database database = metastore.getDatabase(schemaName)
                .orElseThrow(() -> new SchemaNotFoundException(schemaName));

        HdfsContext hdfsContext = new HdfsContext(session, schemaName, tableName);
        Path targetPath = getTableDefaultLocation(database, hdfsContext, hdfsEnvironment, schemaName, tableName);
        Configuration configuration = hdfsEnvironment.getConfiguration(hdfsContext, targetPath);

        MetacatIcebergCatalog catalog = icebergUtil.getCatalog(configuration);
        TableIdentifier tableIdentifier = toTableIdentifier(icebergConfig, schemaName, tableName);
        catalog.createTable(tableIdentifier, schema, partitionSpec, null, null);
        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(schemaName, tableName, configuration, metastore);
        //this.transaction = catalog.newCreateTableTransaction(toTableIdentifier(icebergConfig, schemaName, tableName), schema, partitionSpec, null, null);
        this.transaction = icebergTable.newTransaction();

        return new IcebergWritableTableHandle(
                schemaName,
                tableName,
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                getColumns(icebergTable.schema(), icebergTable.spec(), typeManager),
                getDataPath(targetPath.toString()),
                getFileFormat(tableMetadata.getProperties()));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return finishInsert(session, (IcebergWritableTableHandle) tableHandle, fragments, computedStatistics);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Configuration configuration = getConfiguration(session, table.getSchemaName());
        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(table.getSchemaName(), table.getTableName(), configuration, metastore);

        transaction = icebergTable.newTransaction();

        return new IcebergWritableTableHandle(
                table.getSchemaName(),
                table.getTableName(),
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                getColumns(icebergTable.schema(), icebergTable.spec(), typeManager),
                getDataPath(icebergTable.location()),
                getFileFormat(icebergTable));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;
        org.apache.iceberg.Table icebergTable = transaction.table();
        List<CommitTaskData> commitTasks = new ArrayList<>();

        if (!fragments.isEmpty()) {
            commitTasks = fragments.stream()
                    .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                    .collect(toImmutableList());

            Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                    .map(field -> field.transform().getResultType(
                            icebergTable.schema().findType(field.sourceId())))
                    .toArray(Type[]::new);

            AppendFiles appendFiles = transaction.newFastAppend();
            for (CommitTaskData task : commitTasks) {
                DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                        .withInputFile(HadoopInputFile.fromLocation(task.getPath(), getConfiguration(session, table.getSchemaName())))
                        .withFormat(table.getFileFormat())
                        .withMetrics(task.getMetrics().metrics());

                if (!icebergTable.spec().fields().isEmpty()) {
                    String partitionDataJson = task.getPartitionDataJson()
                            .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                    builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
                }

                appendFiles.appendFile(builder.build());
            }
            appendFiles.commit();
        }

        transaction.commitTransaction();
        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
                .map(CommitTaskData::getPath)
                .collect(toImmutableList())));
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        return Optional.of(new IcebergInputInfo(table.getSnapshotId()));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        metastore.dropTable(handle.getSchemaName(), handle.getTableName(), true);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        metastore.renameTable(handle.getSchemaName(), handle.getTableName(), newTable.getSchemaName(), newTable.getTableName());
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Configuration configuration = getConfiguration(session, handle.getSchemaName());
        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(handle.getSchemaName(), handle.getTableName(), configuration, metastore);
        icebergTable.updateSchema().addColumn(column.getName(), toIcebergType(column.getType())).commit();
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        HiveColumnHandle handle = (HiveColumnHandle) column;
        Configuration configuration = getConfiguration(session, icebergTableHandle.getSchemaName());
        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(icebergTableHandle.getSchemaName(), icebergTableHandle.getTableName(), configuration, metastore);
        icebergTable.updateSchema().deleteColumn(handle.getName()).commit();
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        HiveColumnHandle columnHandle = (HiveColumnHandle) source;
        Configuration configuration = getConfiguration(session, icebergTableHandle.getSchemaName());
        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(icebergTableHandle.getSchemaName(), icebergTableHandle.getTableName(), configuration, metastore);
        icebergTable.updateSchema().renameColumn(columnHandle.getName(), target).commit();
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName schemaTableName)
    {
        String schema = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        if (!metastore.getTable(schema, tableName).isPresent()) {
            throw new TableNotFoundException(schemaTableName);
        }

        Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsContext(session, schema), new Path("file:///tmp"));

        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(schema, tableName, configuration, metastore);

        List<ColumnMetadata> columns = getColumnMetadatas(icebergTable);

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(FILE_FORMAT_PROPERTY, getFileFormat(icebergTable));
        if (!icebergTable.spec().fields().isEmpty()) {
            properties.put(PARTITIONING_PROPERTY, toPartitionFields(icebergTable.spec()));
        }

        return new ConnectorTableMetadata(schemaTableName, columns, properties.build(), Optional.empty());
    }

    private List<ColumnMetadata> getColumnMetadatas(org.apache.iceberg.Table table)
    {
        ImmutableList.Builder builder = new ImmutableList.Builder();

        builder.addAll(table.schema().columns().stream()
                .map(column -> new ColumnMetadata(column.name(), toPrestoType(column.type(), typeManager), column.doc(), false))
                .collect(toImmutableList()));
        builder.add(new ColumnMetadata(PATH_COLUMN_NAME, TypeConverter.toPrestoType(Types.StringType.get(), typeManager), null, true));
        return builder.build();
    }

    private static List<NestedField> toIceberg(List<ColumnMetadata> columns)
    {
        List<NestedField> icebergColumns = new ArrayList<>();
        for (ColumnMetadata column : columns) {
            if (!column.isHidden()) {
                int index = icebergColumns.size();
                Type type = toIcebergType(column.getType());
                NestedField field = column.isNullable()
                        ? NestedField.optional(index, column.getName(), type, column.getComment())
                        : NestedField.required(index, column.getName(), type, column.getComment());
                icebergColumns.add(field);
            }
        }
        return icebergColumns;
    }

    private Configuration getConfiguration(ConnectorSession session, String schemaName)
    {
        return hdfsEnvironment.getConfiguration(new HdfsContext(session, schemaName), new Path("file:///tmp"));
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return updateRowIdHandle();
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return Optional.of(handle);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector only supports delete where one or more partitions are deleted entirely");
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;

        Configuration configuration = getConfiguration(session, handle.getSchemaName());
        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(handle.getSchemaName(), handle.getTableName(), configuration, metastore);
        icebergTable.newDelete()
                .deleteFromRowFilter(toIcebergExpression(handle.getPredicate(), session))
                .commit();

        // TODO: it should be possible to return number of deleted records
        return OptionalLong.empty();
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    public HiveMetastore getMetastore()
    {
        return metastore;
    }

    public void rollback()
    {
        // TODO: cleanup open transaction
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        boolean isCommonView = isCommonViewSupportEnabled(session);

        Map<String, String> properties;
        if (isCommonView) {
            Map buildProps = new HashMap();
            buildProps.put(PRESTO_VERSION_NAME, prestoVersion);
            buildProps.put(PRESTO_QUERY_ID_NAME, session.getQueryId());
            buildProps.put(CommonViewConstants.ENGINE_VERSION, prestoVersion);
            buildProps.put(CommonViewConstants.GENIE_ID, session.getGenieJobId());
            if (!replace) {
                // Add a default table comment when the view is being created
                buildProps.put(TABLE_COMMENT, "Common View created from Presto");
            }
            properties = ImmutableMap.<String, String>builder()
                    .putAll(buildProps)
                    .build();
        }
        else {
            properties = ImmutableMap.<String, String>builder()
                    .put(TABLE_COMMENT, "Presto View")
                    .put(PRESTO_VIEW_FLAG, "true")
                    .put(PRESTO_VERSION_NAME, prestoVersion)
                    .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                    .build();
        }

        Column dummyColumn = new Column("dummy", HIVE_STRING, Optional.empty());

        String schemaName = viewName.getSchemaName();
        String tableName = viewName.getTableName();
        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setOwner(session.getUser())
                .setTableType(org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW.name())
                .setDataColumns(ImmutableList.of(dummyColumn))
                .setPartitionColumns(ImmutableList.of())
                .setParameters(properties);
        tableBuilder.getStorageBuilder()
                .setStorageFormat(VIEW_STORAGE_FORMAT)
                .setLocation("");

        Optional<Table> existing = metastore.getTable(viewName.getSchemaName(), viewName.getTableName());
        if (!isCommonView) {
            tableBuilder.setViewOriginalText(Optional.of(encodeViewData(definition)))
                    .setViewExpandedText(Optional.of("/* Presto View */"));
            Table table = tableBuilder.build();
            PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(session.getUser());
            if (existing.isPresent()) {
                if (!replace) {
                    throw new ViewAlreadyExistsException(viewName);
                }
                metastore.replaceTable(viewName.getSchemaName(), viewName.getTableName(), table, principalPrivileges);
                return;
            }
            try {
                metastore.createTable(table, principalPrivileges);
            }
            catch (TableAlreadyExistsException e) {
                throw new ViewAlreadyExistsException(e.getTableName());
            }
        }
        else {
            Database database = metastore.getDatabase(schemaName)
                    .orElseThrow(() -> new SchemaNotFoundException(schemaName));
            Configuration configuration = getConfiguration(session, viewName.getSchemaName());
            commonViewUtils.writeCommonViewDefinition(configuration, properties, definition, typeManager,
                    session, viewName,
                    replace, existing.isPresent());
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        Optional<Table> view = metastore.getTable(viewName.getSchemaName(), viewName.getTableName());
        if (!view.isPresent()) {
            throw new ViewNotFoundException(viewName);
        }
        boolean isCommonView = HiveUtil.isCommonView(view.get());
        if (!isCommonViewSupportEnabled(session) && isCommonView) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support common views.");
        }

        if (isCommonView) {
            Configuration configuration = getConfiguration(session, viewName.getSchemaName());
            commonViewUtils.dropView(configuration, session, viewName);
        }
        try {
            metastore.dropTable(viewName.getSchemaName(), viewName.getTableName(), true);
        }
        catch (TableNotFoundException e) {
            throw new ViewNotFoundException(e.getTableName());
        }
    }

    private List<String> listSchemas(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent()) {
            return ImmutableList.of(schemaName.get());
        }
        return listSchemaNames(session);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, optionalSchemaName)) {
            for (String tableName : metastore.getAllViews(schemaName)) {
                tableNames.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return tableNames.build();
    }

    public Optional<ConnectorViewDefinition> getPrestoViewDefinition(Optional<Table> view, SchemaTableName viewName)
    {
        return metastore.getTable(viewName.getSchemaName(), viewName.getTableName())
                .filter(HiveUtil::isPrestoOrCommonView)
                .map(v -> {
                    if ("true".equals(v.getParameters().get(PRESTO_VIEW_FLAG))) {
                        ConnectorViewDefinition definition = decodeViewData(v.getViewOriginalText()
                                .orElseThrow(() -> new PrestoException(HIVE_INVALID_METADATA, "No view original text: " + viewName)));
                        // use owner from table metadata if it exists
                        if (v.getOwner() != null && !definition.isRunAsInvoker()) {
                            definition = new ConnectorViewDefinition(
                                    definition.getOriginalSql(),
                                    definition.getCatalog(),
                                    definition.getSchema(),
                                    definition.getColumns(),
                                    Optional.of(v.getOwner()),
                                    false);
                        }
                        return definition;
                    }
                    else {
                        return new ConnectorViewDefinition(
                                v.getViewOriginalText().orElseThrow(() -> new IllegalStateException("original sql must not be missing")),
                                v.getViewExpandedText(),
                                Optional.of(viewName.getSchemaName()),
                                Optional.of(v.getOwner().replaceAll("@.*", "")));
                    }
                });
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        Optional<Table> view = metastore.getTable(viewName.getSchemaName(), viewName.getTableName());
        if (!view.isPresent()) {
            return Optional.empty();
        }
        boolean isCommonView = HiveUtil.isCommonView(view.get());
        if (!isCommonViewSupportEnabled(session) && isCommonView) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support common views.");
        }

        if (isCommonView) {
            Database database = metastore.getDatabase(viewName.getSchemaName())
                    .orElseThrow(() -> new SchemaNotFoundException(viewName.getSchemaName()));
            Configuration configuration = getConfiguration(session, viewName.getSchemaName());
            return commonViewUtils.decodeCommonViewData(configuration, session, typeManager, viewName);
        }
        return getPrestoViewDefinition(view, viewName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        List<SchemaTableName> tableNames;

        tableNames = listViews(session, schemaName);
        for (SchemaTableName schemaTableName : tableNames) {
            Optional<Table> table = metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
            if (table.isPresent() && HiveUtil.isPrestoOrCommonView(table.get())) {
                Table tbl = table.get();
                Optional<ConnectorViewDefinition> viewDefinition = getView(session, schemaTableName);
                if (viewDefinition.isPresent()) {
                    views.put(schemaTableName, viewDefinition.get());
                }
            }
        }
        return views.build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        if (!isStatisticsEnabled(session)) {
            return TableStatistics.empty();
        }
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Configuration configuration = getConfiguration(session, table.getSchemaName());
        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(table.getSchemaName(), table.getTableName(), configuration, metastore);
        TableScan tableScan = IcebergUtil.getTableScan(session, table.getPredicate(), table.getSnapshotId(), icebergTable);
        Map<String, ColumnHandle> columns = getColumnHandles(session, tableHandle);
        return icebergStatisticsProvider.getTableStatistics(table.getTableName(), table.getSnapshotId(), tableScan, columns, session);
    }
}

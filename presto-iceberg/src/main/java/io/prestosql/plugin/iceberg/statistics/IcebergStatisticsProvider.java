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

package io.prestosql.plugin.iceberg.statistics;

import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.plugin.hive.HiveSessionProperties.isIgnoreCorruptedStatistics;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_CORRUPTED_COLUMN_STATISTICS;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_CORRUPTED_ROW_STATISTICS;
import static java.lang.String.format;

public class IcebergStatisticsProvider
{
    private static final Logger log = Logger.get(IcebergStatisticsProvider.class);

    public TableStatistics getTableStatistics(String tableName, Optional<Long> snapshotId, TableScan tableScan, Map<String, ColumnHandle> columns, ConnectorSession session)
    {
        long rowCount = 0;
        TableStatistics.Builder result = TableStatistics.builder();
        Map<Integer, Long> columnSizes = new HashMap<>();
        Map<Integer, Long> nullValueCounts = new HashMap<>();
        Table icebergTable = tableScan.table();
        if (!icebergTable.spec().fields().isEmpty()) {
          // For partitioned tables, set the row count as total number of records in the table which is a coarse
          // upper bound to avoid planning twice
            if (icebergTable.currentSnapshot().summary() != null && icebergTable.currentSnapshot().summary().get(SnapshotSummary.TOTAL_RECORDS_PROP) != null) {
                result.setRowCount(Estimate.of(Long.parseLong(
                        icebergTable.currentSnapshot().summary().get(SnapshotSummary.TOTAL_RECORDS_PROP))));
            }
        }
        else {
            // Currently we plan twice for non-partitioned tables
            Map<String, Integer> icebergColumnNamesToIds = tableScan.schema().columns().stream().collect(Collectors.toMap(Types.NestedField::name, Types.NestedField::fieldId));
            try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
                for (FileScanTask fileScanTask : fileScanTasks) {
                    final DataFile dataFile = fileScanTask.file();
                    rowCount += dataFile.recordCount();
                    Map<Integer, Long> dataFileColumnSizes = dataFile.columnSizes();
                    Map<Integer, Long> dataFileNullValueCounts = dataFile.nullValueCounts();
                    if (dataFileColumnSizes != null && !dataFileColumnSizes.isEmpty()) {
                        dataFile.columnSizes().forEach(
                                (key, value) -> columnSizes.merge(key, value, (v1, v2) -> v1 + v2));
                    }
                    if (dataFileNullValueCounts != null && !dataFileNullValueCounts.isEmpty()) {
                        dataFile.nullValueCounts().forEach(
                                (key, value) -> nullValueCounts.merge(key, value, (v1, v2) -> v1 + v2));
                    }
                }
                checkStatistics(rowCount >= 0, tableName, snapshotId.orElse(null), "Row count must be greater than or equal to zero. Row count: %s", rowCount);
                result.setRowCount(Estimate.of(rowCount));
                for (Map.Entry<String, ColumnHandle> column : columns.entrySet()) {
                    String columnName = column.getKey();
                    ColumnHandle columnHandle = column.getValue();
                    Long dataSize = columnSizes.get(icebergColumnNamesToIds.get(columnName));
                    Long nullCount = nullValueCounts.get(icebergColumnNamesToIds.get(columnName));
                    ColumnStatistics columnStatistics = validateAndSetColumnStatistics(dataSize, nullCount, rowCount, columnName, tableName, snapshotId.orElse(null));
                    result.setColumnStatistics(columnHandle, columnStatistics);
                }
            }
            catch (PrestoException e) {
                if (e.getErrorCode().equals(ICEBERG_CORRUPTED_COLUMN_STATISTICS.toErrorCode()) && isIgnoreCorruptedStatistics(session)) {
                    log.error(e);
                    return TableStatistics.empty();
                }
                throw e;
            }
            catch (IOException e) {
                throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, e);
            }
        }
        return result.build();
    }

    private static ColumnStatistics validateAndSetColumnStatistics(Long dataSize, Long nullCount, long rowCount, String columnName, String tableName, Long snapshotId)
    {
        ColumnStatistics.Builder columnStatisticsBuilder = ColumnStatistics.builder();
        if (dataSize == null) {
            handleMissingStatsException(tableName, columnName, snapshotId, "Column Size");
        }
        else {
            checkColumnStatistics(dataSize >= 0, tableName, columnName, snapshotId, "Column size must be greater than or equal to zero: %s", dataSize);
            columnStatisticsBuilder.setDataSize(Estimate.of(dataSize));
        }

        if (nullCount == null) {
            handleMissingStatsException(tableName, columnName, snapshotId, "Null Value Counts");
        }
        else {
            checkColumnStatistics(nullCount <= rowCount, tableName, columnName, snapshotId, "Nulls count must be lesser than or equal to rowCount. Null value count: %s, Row value count: %s", nullCount, rowCount);

            if (rowCount == 0) {
                columnStatisticsBuilder.setNullsFraction(Estimate.of(0));
            }
            else {
                columnStatisticsBuilder.setNullsFraction(Estimate.of((double) nullCount / rowCount));
            }
        }
        return columnStatisticsBuilder.build();
    }

    private static void handleMissingStatsException(String tableName, String columnName, Long snapshotId, String message)
    {
        log.warn(format("Missing %s statistics for Table: %s, Column: %s, Snapshot Id: %s", message, tableName, columnName, snapshotId));
    }

    private static void checkColumnStatistics(boolean expression, String tableName, String columnName, Long snapshotId, String message, Object... args)
    {
        if (!expression) {
            throw new PrestoException(
                ICEBERG_CORRUPTED_COLUMN_STATISTICS,
                format("Corrupt statistics for [Table: %s, Column: %s, Snapshot Id: %s]: %s", tableName, columnName, snapshotId, format(message, args)));
        }
    }

    private static void checkStatistics(boolean expression, String tableName, Long snapshotId, String message, Object... args)
    {
        if (!expression) {
            throw new PrestoException(
                ICEBERG_CORRUPTED_ROW_STATISTICS,
                format("Corrupt statistics for [Table: %s, Column: %s, Snapshot Id: %s]: %s", tableName, snapshotId, format(message, args)));
        }
    }
}

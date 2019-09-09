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

import io.airlift.json.JsonCodec;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.common.ViewConfig;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import static io.prestosql.plugin.hive.metastore.CachingHiveMetastore.memoizeMetastore;
import static java.util.Objects.requireNonNull;

public class IcebergMetadataFactory
{
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final String prestoVersion;
    private final long perTransactionCacheMaximumSize;
    private final IcebergConfig icebergConfig;
    private final IcebergUtil icebergUtil;
    private final ViewConfig viewConfig;

    @Inject
    public IcebergMetadataFactory(
            HiveConfig config,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskDataJsonCodec,
            IcebergConfig icebergConfig,
            IcebergUtil icebergUtil,
            NodeVersion nodeVersion,
            ViewConfig viewConfig)
    {
        this(config, metastore,
                hdfsEnvironment,
                typeManager,
                commitTaskDataJsonCodec,
                config.getPerTransactionMetastoreCacheMaximumSize(),
                icebergConfig,
                icebergUtil,
                nodeVersion.toString(),
                viewConfig);
    }

    public IcebergMetadataFactory(
            HiveConfig config,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec,
            long perTransactionCacheMaximumSize,
            IcebergConfig icebergConfig,
            IcebergUtil icebergUtil,
            String prestoVersion,
            ViewConfig viewConfig)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.prestoVersion = requireNonNull(prestoVersion, "prestoVersion is null");
        this.perTransactionCacheMaximumSize = perTransactionCacheMaximumSize;
        this.icebergConfig = icebergConfig;
        this.icebergUtil = icebergUtil;
        this.viewConfig = viewConfig;
    }

    public IcebergMetadata create()
    {
        return new IcebergMetadata(
                memoizeMetastore(metastore, perTransactionCacheMaximumSize),
                hdfsEnvironment,
                typeManager,
                commitTaskCodec,
                icebergConfig,
                icebergUtil,
                prestoVersion,
                viewConfig);
    }
}

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
package io.prestosql.plugin.hive.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.huawei.boostkit.omnidata.decode.impl.OpenLooKengDeserializer;
import com.huawei.boostkit.omnidata.model.TaskSource;
import com.huawei.boostkit.omnidata.model.datasource.DataSource;
import com.huawei.boostkit.omnidata.reader.DataReader;
import com.huawei.boostkit.omnidata.reader.DataReaderFactory;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.parquet.ParquetCorruptionException;
import io.prestosql.parquet.ParquetDataSource;
import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.parquet.predicate.Predicate;
import io.prestosql.parquet.reader.MetadataReader;
import io.prestosql.parquet.reader.ParquetReader;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveOffloadExpression;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.HivePartitionKey;
import io.prestosql.plugin.hive.HivePushDownPageSource;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.SplitMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.huawei.boostkit.omnidata.transfer.OmniDataProperty.OMNIDATA_CLIENT_TARGET_LIST;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.parquet.ParquetTypeUtils.getColumnIO;
import static io.prestosql.parquet.ParquetTypeUtils.getDescriptors;
import static io.prestosql.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.prestosql.parquet.predicate.PredicateUtils.buildPredicate;
import static io.prestosql.parquet.predicate.PredicateUtils.predicateMatches;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static io.prestosql.plugin.hive.HiveSessionProperties.getParquetMaxReadBlockSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.isFailOnCorruptedParquetStatistics;
import static io.prestosql.plugin.hive.HiveSessionProperties.isUseParquetColumnNames;
import static io.prestosql.plugin.hive.HiveUtil.getDeserializerClassName;
import static io.prestosql.plugin.hive.HiveUtil.shouldUseRecordReaderFromInputFormat;
import static io.prestosql.plugin.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static io.prestosql.plugin.hive.util.PageSourceUtil.buildPushdownContext;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

public class ParquetPageSourceFactory
        implements HivePageSourceFactory
{
    private static final Set<String> PARQUET_SERDE_CLASS_NAMES = ImmutableSet.<String>builder()
            .add("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
            .add("parquet.hive.serde.ParquetHiveSerDe")
            .build();
    public static final String WRITER_TIME_ZONE_KEY = "writer.time.zone";

    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;

    private final DateTimeZone timeZone;
    private String omniDataServerTarget;

    @Inject
    public ParquetPageSourceFactory(TypeManager typeManager, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats, HiveConfig hiveConfig)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.timeZone = requireNonNull(hiveConfig, "hiveConfig is null").getParquetDateTimeZone();
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<DynamicFilterSupplier> dynamicFilter,
            Optional<DeleteDeltaLocations> deleteDeltaLocations,
            Optional<Long> startRowOffsetOfFile,
            Optional<List<IndexMetadata>> indexes,
            SplitMetadata splitMetadata,
            boolean splitCacheable,
            long dataSourceLastModifiedTime,
            List<HivePartitionKey> partitionKeys,
            OptionalInt bucketNumber,
            Optional<String> omniDataAddress,
            HiveOffloadExpression offloadExpression)
    {
        if (!PARQUET_SERDE_CLASS_NAMES.contains(getDeserializerClassName(
                schema))) {
            return Optional.empty();
        }

        omniDataAddress.ifPresent(s -> omniDataServerTarget = s);

        if (offloadExpression.isPresent()) {
            checkArgument(omniDataAddress.isPresent(), "omniDataAddress is empty");
        }

        if (HiveSessionProperties.isOmniDataEnabled(session)
                && omniDataAddress.isPresent()
                && offloadExpression.isPresent()) {
            com.huawei.boostkit.omnidata.model.Predicate predicate =
                    buildPushdownContext(columns, offloadExpression, typeManager,
                            effectivePredicate, partitionKeys, bucketNumber, path);
            return Optional.of(createParquetPushDownPageSource(path, start, length, predicate));
        }

        return createPageSource(
                configuration,
                session,
                path,
                start,
                length,
                fileSize,
                schema,
                columns,
                effectivePredicate,
                dynamicFilter,
                deleteDeltaLocations,
                startRowOffsetOfFile,
                indexes,
                splitMetadata,
                splitCacheable,
                dataSourceLastModifiedTime);
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<DynamicFilterSupplier> dynamicFilter,
            Optional<DeleteDeltaLocations> deleteDeltaLocations,
            Optional<Long> startRowOffsetOfFile,
            Optional<List<IndexMetadata>> indexes,
            SplitMetadata splitMetadata,
            boolean splitCacheable,
            long dataSourceLastModifiedTime)
    {
        if (!PARQUET_SERDE_CLASS_NAMES.contains(getDeserializerClassName(schema)) || shouldUseRecordReaderFromInputFormat(configuration, schema)) {
            return Optional.empty();
        }

        checkArgument(!deleteDeltaLocations.isPresent(), "Delete delta is not supported");

        return Optional.of(createParquetPageSource(
                hdfsEnvironment,
                session.getUser(),
                configuration,
                path,
                start,
                length,
                fileSize,
                schema,
                columns,
                isUseParquetColumnNames(session),
                isFailOnCorruptedParquetStatistics(session),
                getParquetMaxReadBlockSize(session),
                typeManager,
                effectivePredicate,
                stats,
                timeZone));
    }

    public static ParquetPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            boolean useParquetColumnNames,
            boolean failOnCorruptedParquetStatistics,
            DataSize maxReadBlockSize,
            TypeManager typeManager,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            FileFormatDataSourceStats stats,
            DateTimeZone timeZone)
    {
        AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        DateTimeZone readerTimeZone = timeZone;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(user, () -> fileSystem.open(path));
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(inputStream, path, fileSize);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();
            dataSource = buildHdfsParquetDataSource(inputStream, path, fileSize, stats);
            String writerTimeZoneId = fileMetaData.getKeyValueMetaData().get(WRITER_TIME_ZONE_KEY);
            if (writerTimeZoneId != null && !writerTimeZoneId.equalsIgnoreCase(readerTimeZone.getID())) {
                readerTimeZone = DateTimeZone.forID(writerTimeZoneId);
            }

            List<org.apache.parquet.schema.Type> fields = columns.stream()
                    .filter(column -> column.getColumnType() == REGULAR)
                    .map(column -> getParquetType(column, fileSchema, useParquetColumnNames))
                    .filter(Objects::nonNull)
                    .collect(toList());

            MessageType requestedSchema = new MessageType(fileSchema.getName(), fields);

            ImmutableList.Builder<BlockMetaData> footerBlocks = ImmutableList.builder();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= start && firstDataPage < start + length) {
                    footerBlocks.add(block);
                }
            }

            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath);
            final ParquetDataSource finalDataSource = dataSource;
            ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
            for (BlockMetaData block : footerBlocks.build()) {
                if (predicateMatches(parquetPredicate, block, finalDataSource, descriptorsByPath, parquetTupleDomain, failOnCorruptedParquetStatistics)) {
                    blocks.add(block);
                }
            }
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    messageColumnIO,
                    blocks.build(),
                    dataSource,
                    readerTimeZone,
                    systemMemoryContext,
                    maxReadBlockSize);

            return new ParquetPageSource(
                    parquetReader,
                    fileSchema,
                    messageColumnIO,
                    typeManager,
                    schema,
                    columns,
                    effectivePredicate,
                    useParquetColumnNames);
        }
        catch (Exception e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            if (e instanceof ParquetCorruptionException) {
                throw new PrestoException(HIVE_BAD_DATA, e);
            }
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            String message = format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            if (e instanceof BlockMissingException) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    public HivePushDownPageSource createParquetPushDownPageSource(
            Path path,
            long start,
            long length,
            com.huawei.boostkit.omnidata.model.Predicate predicate)
    {
        AggregatedMemoryContext systemMemoryUsage = newSimpleAggregatedMemoryContext();
        Properties transProperties = new Properties();
        transProperties.put(OMNIDATA_CLIENT_TARGET_LIST, omniDataServerTarget);

        DataSource parquetPushDownDataSource = new com.huawei.boostkit.omnidata.model.datasource.hdfs.HdfsParquetDataSource(path.toString(), start, length, false);

        TaskSource readTaskInfo = new TaskSource(
                parquetPushDownDataSource,
                predicate,
                TaskSource.ONE_MEGABYTES);
        DataReader<Page> dataReader = DataReaderFactory.create(transProperties, readTaskInfo, new OpenLooKengDeserializer());

        return new HivePushDownPageSource(dataReader, systemMemoryUsage);
    }

    public static TupleDomain<ColumnDescriptor> getParquetTupleDomain(Map<List<String>, RichColumnDescriptor> descriptorsByPath, TupleDomain<HiveColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        for (Entry<HiveColumnHandle, Domain> entry : effectivePredicate.getDomains().get().entrySet()) {
            HiveColumnHandle columnHandle = entry.getKey();
            // skip looking up predicates for complex types as Parquet only stores stats for primitives
            if (!columnHandle.getHiveType().getCategory().equals(PRIMITIVE)) {
                continue;
            }

            RichColumnDescriptor descriptor = descriptorsByPath.get(ImmutableList.of(columnHandle.getName()));
            if (descriptor != null) {
                predicate.put(descriptor, entry.getValue());
            }
        }
        return TupleDomain.withColumnDomains(predicate.build());
    }

    public static org.apache.parquet.schema.Type getParquetType(HiveColumnHandle column, MessageType messageType, boolean useParquetColumnNames)
    {
        if (useParquetColumnNames) {
            return getParquetTypeByName(column.getName(), messageType);
        }

        if (column.getHiveColumnIndex() < messageType.getFieldCount()) {
            return messageType.getType(column.getHiveColumnIndex());
        }
        return null;
    }
}

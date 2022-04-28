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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.prestosql.PagesIndexPageSorter;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.PagesIndex;
import io.prestosql.orc.OrcCacheStore;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.gcs.GoogleGcsConfigurationInitializer;
import io.prestosql.plugin.hive.gcs.HiveGcsConfig;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.plugin.hive.orc.OrcSelectivePageSourceFactory;
import io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory;
import io.prestosql.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.prestosql.plugin.hive.s3.HiveS3Config;
import io.prestosql.plugin.hive.s3.PrestoS3ConfigurationInitializer;
import io.prestosql.plugin.hive.util.IndexCache;
import io.prestosql.plugin.hive.util.IndexCacheLoader;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.testing.NoOpIndexClient;
import io.prestosql.testing.TestingConnectorSession;
import io.prestosql.type.InternalTypeManager;
import org.apache.hadoop.hive.common.type.Timestamp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveType.HIVE_LONG;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.type.Decimals.encodeScaledValue;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.util.stream.Collectors.toList;

public final class HiveTestUtils
{
    private HiveTestUtils()
    {
    }

    public static final ConnectorSession SESSION = new TestingConnectorSession(
            new HiveSessionProperties(new HiveConfig().setOrcLazyReadSmallRanges(false), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());

    public static final Metadata METADATA = createTestMetadataManager();
    public static final TypeManager TYPE_MANAGER = new InternalTypeManager(METADATA.getFunctionAndTypeManager());

    public static final HdfsEnvironment HDFS_ENVIRONMENT = createTestHdfsEnvironment(new HiveConfig());

    public static final PageSorter PAGE_SORTER = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));

    public static Set<HivePageSourceFactory> getDefaultHiveDataStreamFactories(HiveConfig hiveConfig)
    {
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveConfig);
        return ImmutableSet.<HivePageSourceFactory>builder()
                .add(new RcFilePageSourceFactory(TYPE_MANAGER, testHdfsEnvironment, stats, hiveConfig))
                .add(new OrcPageSourceFactory(TYPE_MANAGER, hiveConfig, testHdfsEnvironment, stats, OrcCacheStore.builder().newCacheStore(
                        new HiveConfig().getOrcFileTailCacheLimit(), Duration.ofMillis(new HiveConfig().getOrcFileTailCacheTtl().toMillis()),
                        new HiveConfig().getOrcStripeFooterCacheLimit(),
                        Duration.ofMillis(new HiveConfig().getOrcStripeFooterCacheTtl().toMillis()),
                        new HiveConfig().getOrcRowIndexCacheLimit(), Duration.ofMillis(new HiveConfig().getOrcRowIndexCacheTtl().toMillis()),
                        new HiveConfig().getOrcBloomFiltersCacheLimit(),
                        Duration.ofMillis(new HiveConfig().getOrcBloomFiltersCacheTtl().toMillis()),
                        new HiveConfig().getOrcRowDataCacheMaximumWeight(), Duration.ofMillis(new HiveConfig().getOrcRowDataCacheTtl().toMillis()),
                        new HiveConfig().isOrcCacheStatsMetricCollectionEnabled())))
                .add(new ParquetPageSourceFactory(TYPE_MANAGER, testHdfsEnvironment, stats, new HiveConfig()))
                .build();
    }

    public static HiveRecordCursorProvider createGenericHiveRecordCursorProvider(HdfsEnvironment hdfsEnvironment)
    {
        return new GenericHiveRecordCursorProvider(hdfsEnvironment);
    }

    public static Set<HiveSelectivePageSourceFactory> getDefaultHiveSelectiveFactories(HiveConfig hiveConfig)
    {
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveConfig);
        return ImmutableSet.<HiveSelectivePageSourceFactory>builder()
                .add(new OrcSelectivePageSourceFactory(TYPE_MANAGER, hiveConfig, testHdfsEnvironment, stats, OrcCacheStore.builder().newCacheStore(
                        new HiveConfig().getOrcFileTailCacheLimit(), Duration.ofMillis(new HiveConfig().getOrcFileTailCacheTtl().toMillis()),
                        new HiveConfig().getOrcStripeFooterCacheLimit(),
                        Duration.ofMillis(new HiveConfig().getOrcStripeFooterCacheTtl().toMillis()),
                        new HiveConfig().getOrcRowIndexCacheLimit(), Duration.ofMillis(new HiveConfig().getOrcRowIndexCacheTtl().toMillis()),
                        new HiveConfig().getOrcBloomFiltersCacheLimit(),
                        Duration.ofMillis(new HiveConfig().getOrcBloomFiltersCacheTtl().toMillis()),
                        new HiveConfig().getOrcRowDataCacheMaximumWeight(), Duration.ofMillis(new HiveConfig().getOrcRowDataCacheTtl().toMillis()),
                        new HiveConfig().isOrcCacheStatsMetricCollectionEnabled())))
                .build();
    }

    public static IndexCache getNoOpIndexCache()
    {
        return new IndexCache(new IndexCacheLoader(null), new NoOpIndexClient())
        {
            @Override
            public List<IndexMetadata> getIndices(String catalog, String table, HiveSplit hiveSplit, TupleDomain<HiveColumnHandle> effectivePredicate, List<HiveColumnHandle> partitions)
            {
                return ImmutableList.of();
            }
        };
    }

    public static Set<HiveRecordCursorProvider> getDefaultHiveRecordCursorProvider(HiveConfig hiveConfig)
    {
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveConfig);
        return ImmutableSet.<HiveRecordCursorProvider>builder()
                .add(new GenericHiveRecordCursorProvider(testHdfsEnvironment))
                .build();
    }

    public static Set<HiveFileWriterFactory> getDefaultHiveFileWriterFactories(HiveConfig hiveConfig)
    {
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveConfig);
        return ImmutableSet.<HiveFileWriterFactory>builder()
                .add(new RcFileFileWriterFactory(testHdfsEnvironment, TYPE_MANAGER, new NodeVersion("test_version"), hiveConfig, new FileFormatDataSourceStats()))
                .add(getDefaultOrcFileWriterFactory(hiveConfig))
                .build();
    }

    public static OrcFileWriterFactory getDefaultOrcFileWriterFactory(HiveConfig hiveConfig)
    {
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveConfig);
        return new OrcFileWriterFactory(
                testHdfsEnvironment,
                TYPE_MANAGER,
                new NodeVersion("test_version"),
                hiveConfig,
                new FileFormatDataSourceStats(),
                new OrcFileWriterConfig());
    }

    public static List<Type> getTypes(List<? extends ColumnHandle> columnHandles)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ColumnHandle columnHandle : columnHandles) {
            types.add(METADATA.getType(((HiveColumnHandle) columnHandle).getTypeSignature()));
        }
        return types.build();
    }

    public static HdfsEnvironment createTestHdfsEnvironment(HiveConfig config)
    {
        HdfsConfiguration hdfsConfig = new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(
                        config,
                        ImmutableSet.of(
                                new PrestoS3ConfigurationInitializer(new HiveS3Config()),
                                new GoogleGcsConfigurationInitializer(new HiveGcsConfig()))),
                ImmutableSet.of());
        return new HdfsEnvironment(hdfsConfig, config, new NoHdfsAuthentication());
    }

    public static MapType mapType(Type keyType, Type valueType)
    {
        return (MapType) METADATA.getFunctionAndTypeManager().getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
    }

    public static ArrayType arrayType(Type elementType)
    {
        return (ArrayType) METADATA.getFunctionAndTypeManager().getParameterizedType(
                StandardTypes.ARRAY,
                ImmutableList.of(TypeSignatureParameter.of(elementType.getTypeSignature())));
    }

    public static RowType rowType(List<NamedTypeSignature> elementTypeSignatures)
    {
        return (RowType) METADATA.getFunctionAndTypeManager().getParameterizedType(
                StandardTypes.ROW,
                ImmutableList.copyOf(elementTypeSignatures.stream()
                        .map(TypeSignatureParameter::of)
                        .collect(toList())));
    }

    public static Long shortDecimal(String value)
    {
        return new BigDecimal(value).unscaledValue().longValueExact();
    }

    public static Slice longDecimal(String value)
    {
        return encodeScaledValue(new BigDecimal(value));
    }

    public static MethodHandle distinctFromOperator(Type type)
    {
        FunctionHandle operatorHandle = METADATA.getFunctionAndTypeManager().resolveOperatorFunctionHandle(IS_DISTINCT_FROM, fromTypes(type, type));
        return METADATA.getFunctionAndTypeManager().getBuiltInScalarFunctionImplementation(operatorHandle).getMethodHandle();
    }

    public static boolean isDistinctFrom(MethodHandle handle, Block left, Block right)
    {
        try {
            return (boolean) handle.invokeExact(left, left == null, right, right == null);
        }
        catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    public static Supplier<List<Set<DynamicFilter>>> createTestDynamicFilterSupplier(String filterKey, List<Long> filterValues)
    {
        Supplier<List<Set<DynamicFilter>>> dynamicFilterSupplier = () -> {
            Set<DynamicFilter> dynamicFilters = new HashSet<>();
            ColumnHandle columnHandle = new HiveColumnHandle(filterKey, HIVE_LONG, parseTypeSignature(StandardTypes.BIGINT), 0, PARTITION_KEY, Optional.empty());
            BloomFilter filter = new BloomFilter(1024 * 1024, 0.01);
            filterValues.stream().forEach(value -> filter.add(value));

            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                filter.writeTo(out);
                dynamicFilters.add(new BloomFilterDynamicFilter("filter", columnHandle, out.toByteArray(), DynamicFilter.Type.GLOBAL));
            }
            catch (IOException e) {
                // could be ignored
            }

            return ImmutableList.of(dynamicFilters);
        };

        return dynamicFilterSupplier;
    }

    public static Timestamp hiveTimestamp(LocalDateTime local)
    {
        return Timestamp.ofEpochSecond(local.toEpochSecond(ZoneOffset.UTC), local.getNano());
    }
}

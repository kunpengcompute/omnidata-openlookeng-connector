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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.MoreFiles;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.HiveColumnHandle.ColumnType;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.Storage;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorVacuumTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.resourcegroups.QueryType;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.testing.TestingConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.util.Progressable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.plugin.hive.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.prestosql.plugin.hive.HiveColumnHandle.pathColumnHandle;
import static io.prestosql.plugin.hive.HiveTestUtils.createTestDynamicFilterSupplier;
import static io.prestosql.plugin.hive.HiveTestUtils.createTestHdfsEnvironment;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.plugin.hive.HiveUtil.getRegularColumnHandles;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.spi.predicate.TupleDomain.withColumnDomains;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestBackgroundHiveSplitLoader
{
    private static final int BUCKET_COUNT = 2;

    private static final String SAMPLE_PATH = "hdfs://VOL1:9000/db_name/table_name/000000_0";
    private static final String SAMPLE_PATH_FILTERED = "hdfs://VOL1:9000/db_name/table_name/000000_1";

    private static final Path RETURNED_PATH = new Path(SAMPLE_PATH);
    private static final Path FILTERED_PATH = new Path(SAMPLE_PATH_FILTERED);

    private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-%s"));

    private static final TupleDomain<HiveColumnHandle> RETURNED_PATH_DOMAIN = withColumnDomains(
            ImmutableMap.of(
                    pathColumnHandle(),
                    Domain.singleValue(VARCHAR, utf8Slice(RETURNED_PATH.toString()))));

    private static final List<LocatedFileStatus> TEST_FILES = ImmutableList.of(
            locatedFileStatus(RETURNED_PATH),
            locatedFileStatus(FILTERED_PATH));

    private static final List<Column> PARTITION_COLUMNS = ImmutableList.of(
            new Column("partitionColumn", HIVE_INT, Optional.empty()));
    private static final List<HiveColumnHandle> BUCKET_COLUMN_HANDLES = ImmutableList.of(
            new HiveColumnHandle("col1", HIVE_INT, INTEGER.getTypeSignature(), 0, ColumnType.REGULAR, Optional.empty()));

    private static final Optional<HiveBucketProperty> BUCKET_PROPERTY = Optional.of(
            new HiveBucketProperty(ImmutableList.of("col1"), BUCKETING_V1, BUCKET_COUNT, ImmutableList.of()));

    private static final Table SIMPLE_TABLE = table(ImmutableList.of(), Optional.empty(), ImmutableMap.of());
    private static final Table PARTITIONED_TABLE = table(PARTITION_COLUMNS, BUCKET_PROPERTY, ImmutableMap.of());

    private static final Column TABLE_COLUMN = new Column(
            "column",
            HiveType.HIVE_INT,
            Optional.of("comment"));
    private static final Storage TABLE_STORAGE = new Storage(
            StorageFormat.create("serde", "input", "output"),
            "location",
            Optional.empty(),
            true,
            ImmutableMap.of("param", "value2"));

    @Test
    public void testNoPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                TupleDomain.none());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertEquals(drain(hiveSplitSource).size(), 2);
    }

    @Test
    public void testPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                RETURNED_PATH_DOMAIN);

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testPathFilterOneBucketMatchPartitionedTable()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                RETURNED_PATH_DOMAIN,
                Optional.of(new HiveBucketing.HiveBucketFilter(ImmutableSet.of(0, 1))),
                PARTITIONED_TABLE,
                Optional.of(new HiveBucketHandle(BUCKET_COLUMN_HANDLES, BUCKETING_V1, BUCKET_COUNT, BUCKET_COUNT)));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testPathFilterBucketedPartitionedTable()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                RETURNED_PATH_DOMAIN,
                Optional.empty(),
                PARTITIONED_TABLE,
                Optional.of(
                        new HiveBucketHandle(
                                getRegularColumnHandles(PARTITIONED_TABLE),
                                BUCKETING_V1,
                                BUCKET_COUNT,
                                BUCKET_COUNT)));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testEmptyFileWithNoBlocks()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                ImmutableList.of(locatedFileStatusWithNoBlocks(RETURNED_PATH)),
                TupleDomain.none());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        List<HiveSplit> splits = drainSplits(hiveSplitSource);
        assertEquals(splits.size(), 1);
        assertEquals(splits.get(0).getPath(), RETURNED_PATH.toString());
        assertEquals(splits.get(0).getLength(), 0);
    }

    @Test
    public void testNoHangIfPartitionIsOffline()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoaderOfflinePartitions();
        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertThrows(RuntimeException.class, () -> drain(hiveSplitSource));
        assertThrows(RuntimeException.class, () -> hiveSplitSource.isFinished());
    }

    @Test
    public void testCachedDirectoryLister()
            throws Exception
    {
        CachingDirectoryLister cachingDirectoryLister = new CachingDirectoryLister(new Duration(5, TimeUnit.MINUTES), 1000, ImmutableList.of("test_dbname.test_table"));
        assertEquals(cachingDirectoryLister.getRequestCount(), 0);

        int totalCount = 1000;
        CountDownLatch firstVisit = new CountDownLatch(1);
        List<Future<List<HiveSplit>>> futures = new ArrayList<>();

        futures.add(EXECUTOR.submit(() -> {
            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(TEST_FILES, cachingDirectoryLister);
            HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
            backgroundHiveSplitLoader.start(hiveSplitSource);
            try {
                return drainSplits(hiveSplitSource);
            }
            finally {
                firstVisit.countDown();
            }
        }));

        for (int i = 0; i < totalCount - 1; i++) {
            futures.add(EXECUTOR.submit(() -> {
                firstVisit.await();
                BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(TEST_FILES, cachingDirectoryLister);
                HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
                backgroundHiveSplitLoader.start(hiveSplitSource);
                return drainSplits(hiveSplitSource);
            }));
        }

        for (Future<List<HiveSplit>> future : futures) {
            assertEquals(future.get().size(), TEST_FILES.size());
        }
        assertEquals(cachingDirectoryLister.getRequestCount(), totalCount);
        assertEquals(cachingDirectoryLister.getHitCount(), totalCount - 1);
        assertEquals(cachingDirectoryLister.getMissCount(), 1);
    }

    @Test
    public void testGetBucketNumber()
    {
        assertEquals(HiveUtil.getBucketNumber("0234_0"), OptionalInt.of(234));
        assertEquals(HiveUtil.getBucketNumber("000234_0"), OptionalInt.of(234));
        assertEquals(HiveUtil.getBucketNumber("0234_99"), OptionalInt.of(234));
        assertEquals(HiveUtil.getBucketNumber("0234_0.txt"), OptionalInt.of(234));
        assertEquals(HiveUtil.getBucketNumber("0234_0_copy_1"), OptionalInt.of(234));
        assertEquals(HiveUtil.getBucketNumber("20190526_072952_00009_fn7s5_bucket-00234"), OptionalInt.of(234));
        assertEquals(HiveUtil.getBucketNumber("20190526_072952_00009_fn7s5_bucket-00234.txt"), OptionalInt.of(234));
        assertEquals(HiveUtil.getBucketNumber("20190526_235847_87654_fn7s5_bucket-56789"), OptionalInt.of(56789));

        assertEquals(HiveUtil.getBucketNumber("234_99"), OptionalInt.empty());
        assertEquals(HiveUtil.getBucketNumber("0234.txt"), OptionalInt.empty());
        assertEquals(HiveUtil.getBucketNumber("0234.txt"), OptionalInt.empty());
    }

    @Test(dataProvider = "testPropagateExceptionDataProvider", timeOut = 60_000)
    public void testPropagateException(boolean error, int threads)
    {
        AtomicBoolean iteratorUsedAfterException = new AtomicBoolean();
        AtomicBoolean isFirstTime = new AtomicBoolean(true);

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                () -> new Iterator<HivePartitionMetadata>()
                {
                    private boolean threw;

                    @Override
                    public boolean hasNext()
                    {
                        iteratorUsedAfterException.compareAndSet(false, threw);
                        return !threw;
                    }

                    @Override
                    public HivePartitionMetadata next()
                    {
                        // isFirstTime variable is used to skip throwing exception from next method called in BackgroundHiveSplitLoader constructor
                        if (!isFirstTime.compareAndSet(true, false)) {
                            iteratorUsedAfterException.compareAndSet(false, threw);
                            threw = true;
                            if (error) {
                                throw new Error("loading error occurred");
                            }
                            throw new RuntimeException("loading error occurred");
                        }
                        return new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                ImmutableMap.of());
                    }
                },
                TupleDomain.all(),
                BackgroundHiveSplitLoader.BucketSplitInfo.createBucketSplitInfo(Optional.empty(), Optional.empty()),
                HiveTestUtils.SESSION,
                new TestingHdfsEnvironment(TEST_FILES),
                new NamenodeStats(),
                new CachingDirectoryLister(new HiveConfig()),
                EXECUTOR,
                threads,
                false,
                Optional.empty(),
                null,
                Optional.empty(),
                Collections.emptyMap(), null);

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertThatThrownBy(() -> drain(hiveSplitSource))
                .hasMessageEndingWith("loading error occurred");

        assertThatThrownBy(hiveSplitSource::isFinished)
                .hasMessageEndingWith("loading error occurred");

        if (threads == 1) {
            assertFalse(iteratorUsedAfterException.get());
        }
    }

    @DataProvider
    public Object[][] testPropagateExceptionDataProvider()
    {
        return new Object[][] {
                {false, 1},
                {true, 1},
                {false, 2},
                {true, 2},
                {false, 4},
                {true, 4},
        };
    }

    @Test
    public void testSplitsGenerationWithAbortedTransactions()
            throws Exception
    {
        ImmutableMap<String, String> tableParams = ImmutableMap.of(
                "transactional", "true",
                "transactional_properties", "insert_only");
        testAbortedTransactions(tableParams, Optional.empty(), ImmutableMap.of());
    }

    @Test
    public void testSplitsGenerationWithAbortedTransactionsForIUDTable()
            throws Exception
    {
        ImmutableMap<String, String> tableParams = ImmutableMap.of(
                "transactional", "true");
        testAbortedTransactions(tableParams, Optional.empty(), ImmutableMap.of());
    }

    @Test
    public void testSplitsGenerationWithAbortedTransactionsForVacuum()
            throws Exception
    {
        ImmutableMap<String, String> tableParams = ImmutableMap.of(
                "transactional", "true");
        testAbortedTransactions(tableParams, Optional.of(QueryType.VACUUM), ImmutableMap.of("FULL", false));
    }

    private void testAbortedTransactions(ImmutableMap<String, String> tableParameters,
            Optional<QueryType> queryType, ImmutableMap<String, Object> queryInfo)
            throws Exception
    {
        java.nio.file.Path tablePath = Files.createTempDirectory(UUID.randomUUID().toString());
        Table table = table(
                tablePath.toString(),
                ImmutableList.of(),
                Optional.empty(),
                tableParameters);

        List<String> filePaths = ImmutableList.of(
                tablePath + "/delta_0000001_0000001_0000/_orc_acid_version",
                tablePath + "/delta_0000001_0000001_0000/bucket_00000",
                tablePath + "/delta_0000002_0000002_0000/_orc_acid_version",
                tablePath + "/delta_0000002_0000002_0000/bucket_00000",
                tablePath + "/delta_0000003_0000003_0000/_orc_acid_version",
                tablePath + "/delta_0000003_0000003_0000/bucket_00000");

        try {
            for (String path : filePaths) {
                File file = new File(path);
                assertTrue(file.getParentFile().exists() || file.getParentFile().mkdirs(), "Failed creating directory " + file.getParentFile());
                createOrcAcidFile(file);
            }

            // ValidWriteIdList is of format <currentTxn>$<schema>.<table>:<highWatermark>:<minOpenWriteId>::<AbortedTxns>
            // This writeId list has high watermark transaction=3 and aborted transaction=2
            String validWriteIdsList = format("4$%s.%s:3:9223372036854775807::2", table.getDatabaseName(), table.getTableName());

            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                    createTestHdfsEnvironment(new HiveConfig()),
                    TupleDomain.none(),
                    Optional.empty(),
                    table,
                    Optional.empty(),
                    Optional.of(new ValidReaderWriteIdList(validWriteIdsList)),
                    queryType,
                    queryInfo);

            HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
            backgroundHiveSplitLoader.start(hiveSplitSource);
            List<String> splits = drain(hiveSplitSource);
            assertTrue(splits.stream().anyMatch(p -> p.contains(filePaths.get(1))), format("%s not found in splits %s", filePaths.get(1), splits));
            assertTrue(splits.stream().anyMatch(p -> p.contains(filePaths.get(5))), format("%s not found in splits %s", filePaths.get(5), splits));
            assertFalse(splits.stream().anyMatch(p -> p.contains(filePaths.get(3))), format("Aborted txn %s found in splits %s", filePaths.get(3), splits));
        }
        finally {
            Files.walk(tablePath).sorted(Comparator.reverseOrder()).map(java.nio.file.Path::toFile).forEach(File::delete);
        }
    }

    private static void createOrcAcidFile(File file)
            throws IOException
    {
        if (file.getName().equals("_orc_acid_version")) {
            Files.write(file.toPath(), "2".getBytes(UTF_8));
            return;
        }
        checkState(file.createNewFile(), "Failed to create file %s", file);
    }

    @Test
    public void testFullAcidTableWithOriginalFilesFails()
            throws Exception
    {
        java.nio.file.Path tablePath = Files.createTempDirectory("TestBackgroundHiveSplitLoader");
        Table table = table(
                tablePath.toString(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableMap.of("transactional", "true"));

        String originalFile = tablePath + "/000000_1";
        List<String> filePaths = ImmutableList.of(
                tablePath + "/delta_0000002_0000002_0000/_orc_acid_version",
                tablePath + "/delta_0000002_0000002_0000/bucket_00000");

        try {
            for (String path : filePaths) {
                File file = new File(path);
                assertTrue(file.getParentFile().exists() || file.getParentFile().mkdirs(), "Failed creating directory " + file.getParentFile());
                createOrcAcidFile(file);
            }
            Files.write(Paths.get(originalFile), "test".getBytes(UTF_8));

            // ValidWriteIdsList is of format <currentTxn>$<schema>.<table>:<highWatermark>:<minOpenWriteId>::<AbortedTxns>
            // This writeId list has high watermark transaction=3
            ValidReaderWriteIdList validWriteIdsList = new ValidReaderWriteIdList(format("4$%s.%s:3:9223372036854775807::", table.getDatabaseName(), table.getTableName()));

            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                    createTestHdfsEnvironment(new HiveConfig()),
                    TupleDomain.all(),
                    Optional.empty(),
                    table,
                    Optional.empty(),
                    Optional.of(validWriteIdsList));

            HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
            backgroundHiveSplitLoader.start(hiveSplitSource);
            drain(hiveSplitSource);
            assertThatThrownBy(() -> drain(hiveSplitSource))
                    .isInstanceOfSatisfying(PrestoException.class, e -> {
                        assertEquals(NOT_SUPPORTED.toErrorCode(), e.getErrorCode());
                    });
        }
        catch (PrestoException e) {
            assertEquals(NOT_SUPPORTED.toErrorCode(), e.getErrorCode(), "Unexpected exception " + e);
        }
        finally {
            MoreFiles.deleteRecursively(tablePath, ALLOW_INSECURE);
        }
    }

    @Test
    public void testFullAcidTableVacuumWithOpenTxns()
            throws Exception
    {
        java.nio.file.Path tablePath = Files.createTempDirectory("TestBackgroundHiveSplitLoader");
        Table table = table(
                tablePath.toString(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableMap.of("transactional", "true"));

        List<String> filePaths = ImmutableList.of(
                tablePath + "/delta_0000001_0000001_0000/_orc_acid_version",
                tablePath + "/delta_0000001_0000001_0000/bucket_00000",
                tablePath + "/delta_0000002_0000002_0000/_orc_acid_version",
                tablePath + "/delta_0000002_0000002_0000/bucket_00000",
                tablePath + "/delta_0000003_0000003_0000/_orc_acid_version",
                tablePath + "/delta_0000003_0000003_0000/bucket_00000");

        try {
            for (String path : filePaths) {
                File file = new File(path);
                assertTrue(file.getParentFile().exists() || file.getParentFile().mkdirs(), "Failed creating directory " + file.getParentFile());
                createOrcAcidFile(file);
            }

            // ValidWriteIdsList is of format <currentTxn>$<schema>.<table>:<highWatermark>:<minOpenWriteId>::<AbortedTxns>
            // This writeId list has high watermark transaction=3
            ValidReaderWriteIdList validWriteIdsList = new ValidReaderWriteIdList(format("4$%s.%s:1:2::", table.getDatabaseName(), table.getTableName()));

            ImmutableMap<String, Object> queryInfo = ImmutableMap.of(
                    "FULL", false,
                    "vacuumHandle", new ConnectorVacuumTableHandle() {});
            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                    createTestHdfsEnvironment(new HiveConfig()),
                    TupleDomain.all(),
                    Optional.empty(),
                    table,
                    Optional.empty(),
                    Optional.of(validWriteIdsList),
                    Optional.of(QueryType.VACUUM),
                    queryInfo);

            HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
            backgroundHiveSplitLoader.start(hiveSplitSource);
            List<HiveSplit> splits = drainSplits(hiveSplitSource);
            assertEquals(1, splits.size());
        }
        finally {
            MoreFiles.deleteRecursively(tablePath, ALLOW_INSECURE);
        }
    }

    @Test
    public void testHive2VersionedFullAcidTableFails()
            throws Exception
    {
        java.nio.file.Path tablePath = Files.createTempDirectory("TestBackgroundHiveSplitLoader");
        Table table = table(
                tablePath.toString(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableMap.of("transactional", "true"));

        List<String> filePaths = ImmutableList.of(
                tablePath + "/000000_1", // _orc_acid_version does not exist so it's assumed to be "ORC ACID version 0"
                tablePath + "/delta_0000002_0000002_0000/bucket_00000");

        for (String path : filePaths) {
            File file = new File(path);
            assertTrue(file.getParentFile().exists() || file.getParentFile().mkdirs(), "Failed creating directory " + file.getParentFile());
            createOrcAcidFile(file);
        }

        // ValidWriteIdsList is of format <currentTxn>$<schema>.<table>:<highWatermark>:<minOpenWriteId>::<AbortedTxns>
        // This writeId list has high watermark transaction=3
        ValidReaderWriteIdList validWriteIdsList = new ValidReaderWriteIdList(format("4$%s.%s:3:9223372036854775807::", table.getDatabaseName(), table.getTableName()));

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                createTestHdfsEnvironment(new HiveConfig()),
                TupleDomain.all(),
                Optional.empty(),
                table,
                Optional.empty(),
                Optional.of(validWriteIdsList));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        assertThatThrownBy(() -> drain(hiveSplitSource))
                .isInstanceOfSatisfying(PrestoException.class, e -> assertEquals(NOT_SUPPORTED.toErrorCode(), e.getErrorCode()))
                .hasMessage("Hive transactional tables are supported with Hive 3.0 and only after a major compaction has been run");

        MoreFiles.deleteRecursively(tablePath, ALLOW_INSECURE);
    }

    @Test
    public void testPartitionedTableWithDynamicFilter()
            throws Exception
    {
        TypeManager typeManager = new TestingTypeManager();
        List<HivePartitionMetadata> hivePartitionMetadatas =
                ImmutableList.of(
                        new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.of(new Partition(
                                        "testSchema",
                                        "table_name",
                                        ImmutableList.of("1"),
                                        TABLE_STORAGE,
                                        ImmutableList.of(TABLE_COLUMN),
                                        ImmutableMap.of("param", "value"))),
                                ImmutableMap.of()));

        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveConfig()
                                .setMaxSplitSize(new DataSize(1.0, GIGABYTE))
                                .setDynamicFilterPartitionFilteringEnabled(true),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig()).getSessionProperties());

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = new BackgroundHiveSplitLoader(
                PARTITIONED_TABLE,
                hivePartitionMetadatas,
                TupleDomain.all(),
                BackgroundHiveSplitLoader.BucketSplitInfo.createBucketSplitInfo(Optional.empty(), Optional.empty()),
                connectorSession,
                new TestingHdfsEnvironment(TEST_FILES),
                new NamenodeStats(),
                new CachingDirectoryLister(new HiveConfig()),
                directExecutor(),
                2,
                false,
                Optional.empty(),
                createTestDynamicFilterSupplier("partitionColumn", ImmutableList.of(0L, 2L, 3L)),
                Optional.empty(),
                ImmutableMap.of(),
                typeManager);

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        List<HiveSplit> splits = drainSplits(hiveSplitSource);
        assertEquals(splits.size(), 0, "Splits should be filtered");
    }

    private static List<String> drain(HiveSplitSource source)
            throws Exception
    {
        return drainSplits(source).stream()
                .map(HiveSplit::getPath)
                .collect(toImmutableList());
    }

    private static List<HiveSplit> drainSplits(HiveSplitSource source)
            throws Exception
    {
        ImmutableList.Builder<HiveSplit> splits = ImmutableList.builder();
        while (!source.isFinished()) {
            source.getNextBatch(NOT_PARTITIONED, 100).get()
                    .getSplits().stream()
                    .map(HiveSplitWrapper::getOnlyHiveSplit)
                    .forEach(splits::add);
        }
        return splits.build();
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<LocatedFileStatus> files,
            TupleDomain<HiveColumnHandle> tupleDomain)
    {
        return backgroundHiveSplitLoader(
                files,
                tupleDomain,
                Optional.empty(),
                SIMPLE_TABLE,
                Optional.empty());
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<LocatedFileStatus> files,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            Optional<HiveBucketing.HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle)
    {
        return backgroundHiveSplitLoader(
                files,
                compactEffectivePredicate,
                hiveBucketFilter,
                table,
                bucketHandle,
                Optional.empty());
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<LocatedFileStatus> files,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            Optional<HiveBucketing.HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<ValidWriteIdList> validWriteIds)
    {
        return backgroundHiveSplitLoader(
                new TestingHdfsEnvironment(files),
                compactEffectivePredicate,
                hiveBucketFilter,
                table,
                bucketHandle,
                validWriteIds);
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            HdfsEnvironment hdfsEnvironment,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            Optional<HiveBucketing.HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<ValidWriteIdList> validWriteIds)
    {
        return backgroundHiveSplitLoader(hdfsEnvironment, compactEffectivePredicate, hiveBucketFilter, table, bucketHandle, validWriteIds, Optional.empty(), Collections.emptyMap());
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            HdfsEnvironment hdfsEnvironment,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            Optional<HiveBucketing.HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<ValidWriteIdList> validWriteIds,
            Optional<QueryType> queryType, Map<String,
            Object> queryInfo)
    {
        List<HivePartitionMetadata> hivePartitionMetadatas =
                ImmutableList.of(
                        new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                ImmutableMap.of()));

        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(new HiveConfig().setMaxSplitSize(new DataSize(1.0, GIGABYTE)), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());

        return new BackgroundHiveSplitLoader(
                table,
                hivePartitionMetadatas,
                compactEffectivePredicate,
                BackgroundHiveSplitLoader.BucketSplitInfo.createBucketSplitInfo(bucketHandle, hiveBucketFilter),
                connectorSession,
                hdfsEnvironment,
                new NamenodeStats(),
                new CachingDirectoryLister(new HiveConfig()),
                EXECUTOR,
                2,
                false,
                validWriteIds,
                null,
                queryType,
                queryInfo, null);
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(List<LocatedFileStatus> files, DirectoryLister directoryLister)
    {
        List<HivePartitionMetadata> hivePartitionMetadatas = ImmutableList.of(
                new HivePartitionMetadata(
                        new HivePartition(new SchemaTableName("testSchema", "table_name")),
                        Optional.empty(),
                        ImmutableMap.of()));

        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(new HiveConfig().setMaxSplitSize(new DataSize(1.0, GIGABYTE)), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());

        return new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                hivePartitionMetadatas,
                TupleDomain.none(),
                Optional.empty(),
                connectorSession,
                new TestingHdfsEnvironment(files),
                new NamenodeStats(),
                directoryLister,
                EXECUTOR,
                2,
                false,
                Optional.empty(),
                null,
                Optional.empty(),
                Collections.emptyMap(), null);
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoaderOfflinePartitions()
    {
        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(new HiveConfig().setMaxSplitSize(new DataSize(1.0, GIGABYTE)), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());

        return new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                createPartitionMetadataWithOfflinePartitions(),
                TupleDomain.all(),
                BackgroundHiveSplitLoader.BucketSplitInfo.createBucketSplitInfo(Optional.empty(), Optional.empty()),
                connectorSession,
                new TestingHdfsEnvironment(TEST_FILES),
                new NamenodeStats(),
                new CachingDirectoryLister(new HiveConfig()),
                directExecutor(),
                2,
                false,
                Optional.empty(),
                null,
                Optional.empty(),
                Collections.emptyMap(), null);
    }

    private static Iterable<HivePartitionMetadata> createPartitionMetadataWithOfflinePartitions()
            throws RuntimeException
    {
        return () -> new AbstractIterator<HivePartitionMetadata>()
        {
            // This iterator is crafted to return a valid partition for the first calls to
            // hasNext() and next(), and then it should throw for the second call to hasNext()
            private int position = -1;

            @Override
            protected HivePartitionMetadata computeNext()
            {
                position++;
                switch (position) {
                    case 0:
                        return new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                ImmutableMap.of());
                    case 1:
                        throw new RuntimeException("OFFLINE");
                    default:
                        return endOfData();
                }
            }
        };
    }

    private static HiveSplitSource hiveSplitSource(HiveSplitLoader hiveSplitLoader)
    {
        return HiveSplitSource.allAtOnce(
                HiveTestUtils.SESSION,
                SIMPLE_TABLE.getDatabaseName(),
                SIMPLE_TABLE.getTableName(),
                1,
                1,
                new DataSize(32, MEGABYTE),
                Integer.MAX_VALUE,
                hiveSplitLoader,
                EXECUTOR,
                new CounterStat(),
                null,
                null, null, new HiveConfig(),
                HiveStorageFormat.ORC);
    }

    private static Table table(
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            ImmutableMap<String, String> tableParameters)
    {
        return table(partitionColumns,
                bucketProperty,
                tableParameters,
                StorageFormat.create(
                        "com.facebook.hive.orc.OrcSerde",
                        "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
                        "org.apache.hadoop.hive.ql.io.RCFileInputFormat"));
    }

    private static Table table(
            String location,
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            ImmutableMap<String, String> tableParameters)
    {
        return table(location,
                partitionColumns,
                bucketProperty,
                tableParameters,
                StorageFormat.create(
                        "com.facebook.hive.orc.OrcSerde",
                        "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
                        "org.apache.hadoop.hive.ql.io.RCFileInputFormat"));
    }

    private static Table table(
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            Map<String, String> tableParameters,
            StorageFormat storageFormat)
    {
        return table("hdfs://VOL1:9000/db_name/table_name",
                partitionColumns,
                bucketProperty,
                tableParameters,
                storageFormat);
    }

    private static Table table(
            String location,
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            Map<String, String> tableParameters,
            StorageFormat storageFormat)
    {
        Table.Builder tableBuilder = Table.builder();
        tableBuilder.getStorageBuilder()
                .setStorageFormat(
                        StorageFormat.create(
                                "com.facebook.hive.orc.OrcSerde",
                                "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
                                "org.apache.hadoop.hive.ql.io.RCFileInputFormat"))
                .setLocation(location)
                .setSkewed(false)
                .setBucketProperty(bucketProperty);

        return tableBuilder
                .setDatabaseName("test_dbname")
                .setOwner("testOwner")
                .setTableName("test_table")
                .setTableType(TableType.MANAGED_TABLE.toString())
                .setDataColumns(ImmutableList.of(new Column("col1", HIVE_STRING, Optional.empty())))
                .setParameters(tableParameters)
                .setPartitionColumns(partitionColumns)
                .build();
    }

    private static LocatedFileStatus locatedFileStatus(Path path)
    {
        return new LocatedFileStatus(
                0L,
                false,
                0,
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                path,
                new BlockLocation[] {new BlockLocation()});
    }

    private static LocatedFileStatus locatedFileStatusWithNoBlocks(Path path)
    {
        return new LocatedFileStatus(
                0L,
                false,
                0,
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                path,
                new BlockLocation[] {});
    }

    public static class TestingHdfsEnvironment
            extends HdfsEnvironment
    {
        private final List<LocatedFileStatus> files;

        public TestingHdfsEnvironment(List<LocatedFileStatus> files)
        {
            super(
                    new HiveHdfsConfiguration(new HdfsConfigurationInitializer(new HiveConfig()), ImmutableSet.of()),
                    new HiveConfig(),
                    new NoHdfsAuthentication());
            this.files = ImmutableList.copyOf(files);
        }

        @Override
        public FileSystem getFileSystem(String user, Path path, Configuration configuration)
        {
            return new TestingHdfsFileSystem(files);
        }
    }

    private static class TestingHdfsFileSystem
            extends FileSystem
    {
        private final List<LocatedFileStatus> files;

        public TestingHdfsFileSystem(List<LocatedFileStatus> files)
        {
            this.files = ImmutableList.copyOf(files);
        }

        @Override
        public boolean delete(Path f, boolean recursive)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rename(Path src, Path dst)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setWorkingDirectory(Path dir)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus[] listStatus(Path f)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
        {
            return new RemoteIterator<LocatedFileStatus>()
            {
                private final Iterator<LocatedFileStatus> iterator = files.iterator();

                @Override
                public boolean hasNext()
                        throws IOException
                {
                    return iterator.hasNext();
                }

                @Override
                public LocatedFileStatus next()
                        throws IOException
                {
                    return iterator.next();
                }
            };
        }

        @Override
        public FSDataOutputStream create(
                Path f,
                FsPermission permission,
                boolean overwrite,
                int bufferSize,
                short replication,
                long blockSize,
                Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean mkdirs(Path f, FsPermission permission)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataInputStream open(Path f, int bufferSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus getFileStatus(Path f)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Path getWorkingDirectory()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI getUri()
        {
            throw new UnsupportedOperationException();
        }
    }
}

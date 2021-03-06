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
package io.prestosql.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcPredicate;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveErrorCode;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPageSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

//TODO Raghu add the handling for BloomFilter, BufferSize

public class OrcDeleteDeltaPageSource
        implements ConnectorPageSource
{
    private final OrcRecordReader recordReader;
    private final OrcDataSource orcDataSource;
    private final FileFormatDataSourceStats stats;
    private final AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

    private boolean closed;

    public OrcDeleteDeltaPageSource(
            Path path,
            long fileSize,
            String sessionUser,
            Configuration configuration,
            HdfsEnvironment hdfsEnvironment,
            DataSize maxMergeDistance,
            DataSize maxBufferSize,
            DataSize streamBufferSize,
            DataSize maxReadBlockSize,
            DataSize tinyStripeThreshold,
            boolean lazyReadSmallRanges,
            boolean orcBloomFiltersEnabled,
            FileFormatDataSourceStats stats,
            long lastModifiedTime)
    {
        this.stats = requireNonNull(stats, "stats is null");

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(sessionUser, path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(sessionUser, () -> fileSystem.open(path));
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    fileSize,
                    maxMergeDistance,
                    maxReadBlockSize,
                    streamBufferSize,
                    lazyReadSmallRanges,
                    inputStream,
                    stats,
                    lastModifiedTime);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new PrestoException(HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT, openError(e, path), e);
        }

        try {
            OrcReader reader = new OrcReader(orcDataSource, maxMergeDistance, tinyStripeThreshold, maxReadBlockSize);

            OrcPageSourceFactory.verifyAcidSchema(reader, path);
            Map<String, OrcColumn> acidColumns = uniqueIndex(
                    reader.getRootColumn().getNestedColumns(),
                    orcColumn -> orcColumn.getColumnName());
            List<OrcColumn> rowIdColumns = ImmutableList.of(acidColumns.get(OrcPageSourceFactory.ACID_COLUMN_ORIGINAL_TRANSACTION), acidColumns.get(OrcPageSourceFactory.ACID_COLUMN_BUCKET), acidColumns.get(OrcPageSourceFactory.ACID_COLUMN_ROW_ID));

            recordReader = reader.createRecordReader(
                    rowIdColumns,
                    ImmutableList.of(BIGINT, INTEGER, BIGINT),
                    OrcPredicate.TRUE,
                    0,
                    fileSize,
                    UTC,
                    systemMemoryContext,
                    INITIAL_BATCH_SIZE,
                    exception -> OrcPageSource.handleException(orcDataSource.getId(), exception));
        }
        catch (Exception e) {
            try {
                orcDataSource.close();
            }
            catch (IOException ex) {
                e.addSuppressed(ex);
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = openError(e, path);
            if (e instanceof BlockMissingException) {
                throw new PrestoException(HiveErrorCode.HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return orcDataSource.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return orcDataSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page page = recordReader.nextPage();
            if (page == null) {
                close();
            }
            return page;
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw OrcPageSource.handleException(orcDataSource.getId(), e);
        }
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        try {
            stats.addMaxCombinedBytesPerRow(recordReader.getMaxCombinedBytesPerRow());
            recordReader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("orcDataSource", orcDataSource.getId())
                .toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryContext.getBytes();
    }

    private void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }

    private static String openError(Throwable t, Path path)
    {
        return format("Error opening Hive delta delete file %s: %s", path, t.getMessage());
    }
}

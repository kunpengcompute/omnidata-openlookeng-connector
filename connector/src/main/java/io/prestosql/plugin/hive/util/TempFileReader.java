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
package io.prestosql.plugin.hive.util;

import com.google.common.collect.AbstractIterator;
import io.airlift.units.DataSize;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcPredicate;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.plugin.hive.HiveErrorCode;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class TempFileReader
        extends AbstractIterator<Page>
        implements Closeable
{
    private final OrcRecordReader reader;

    public TempFileReader(List<Type> types, OrcDataSource dataSource)
    {
        requireNonNull(types, "types is null");

        try {
            OrcReader orcReader = new OrcReader(
                    dataSource,
                    new DataSize(1, MEGABYTE),
                    new DataSize(8, MEGABYTE),
                    new DataSize(16, MEGABYTE));
            reader = orcReader.createRecordReader(
                    orcReader.getRootColumn().getNestedColumns(),
                    types,
                    OrcPredicate.TRUE,
                    UTC,
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE,
                    TempFileReader::handleException);
        }
        catch (IOException e) {
            throw handleException(e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        reader.close();
    }

    @Override
    protected Page computeNext()
    {
        try {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedIOException();
            }

            Page page = reader.nextPage();
            if (page == null) {
                return endOfData();
            }

            // eagerly load the page
            return page.getLoadedPage();
        }
        catch (IOException e) {
            throw handleException(e);
        }
    }

    private static PrestoException handleException(Exception e)
    {
        return new PrestoException(HiveErrorCode.HIVE_WRITER_DATA_ERROR, "Failed to read temporary data", e);
    }
}

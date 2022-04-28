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
package io.prestosql.plugin.hive.benchmark;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.OutputStreamSliceOutput;
import io.prestosql.orc.OrcCacheStore;
import io.prestosql.orc.OrcWriter;
import io.prestosql.orc.OrcWriterOptions;
import io.prestosql.orc.OrcWriterStats;
import io.prestosql.orc.OutputStreamOrcDataSink;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.GenericHiveRecordCursorProvider;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveCompressionCodec;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.HiveRecordCursorProvider;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.HiveTestUtils;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveTypeName;
import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.plugin.hive.RecordFileWriter;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.plugin.hive.benchmark.HiveFileFormatBenchmark.TestData;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory;
import io.prestosql.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.prestosql.rcfile.AircompressorCodecFactory;
import io.prestosql.rcfile.HadoopCodecFactory;
import io.prestosql.rcfile.RcFileEncoding;
import io.prestosql.rcfile.RcFileWriter;
import io.prestosql.rcfile.binary.BinaryRcFileEncoding;
import io.prestosql.rcfile.text.TextRcFileEncoding;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static io.prestosql.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static io.prestosql.plugin.hive.HdfsConfigurationInitializer.configureCompression;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveTestUtils.TYPE_MANAGER;
import static io.prestosql.plugin.hive.HiveType.toHiveType;
import static io.prestosql.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.joda.time.DateTimeZone.UTC;

public enum FileFormat
{
    PRESTO_RCBINARY {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new RcFilePageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats(), new HiveConfig().setRcfileTimeZone("UTC"));
            return createPageSource(pageSourceFactory, session, targetFile, columnNames, columnTypes, HiveStorageFormat.RCBINARY);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
                throws IOException
        {
            return new PrestoRcFileFormatWriter(
                    targetFile,
                    columnTypes,
                    new BinaryRcFileEncoding(UTC),
                    compressionCodec);
        }
    },
    PRESTO_RCTEXT {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new RcFilePageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats(), new HiveConfig().setRcfileTimeZone("UTC"));
            return createPageSource(pageSourceFactory, session, targetFile, columnNames, columnTypes, HiveStorageFormat.RCTEXT);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
                throws IOException
        {
            return new PrestoRcFileFormatWriter(
                    targetFile,
                    columnTypes,
                    new TextRcFileEncoding(),
                    compressionCodec);
        }
    },

    PRESTO_ORC {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new OrcPageSourceFactory(HiveTestUtils.TYPE_MANAGER, new HiveConfig().setUseOrcColumnNames(false), hdfsEnvironment, new FileFormatDataSourceStats(), OrcCacheStore.builder().newCacheStore(
                    new HiveConfig().getOrcFileTailCacheLimit(), Duration.ofMillis(new HiveConfig().getOrcFileTailCacheTtl().toMillis()),
                    new HiveConfig().getOrcStripeFooterCacheLimit(),
                    Duration.ofMillis(new HiveConfig().getOrcStripeFooterCacheTtl().toMillis()),
                    new HiveConfig().getOrcRowIndexCacheLimit(), Duration.ofMillis(new HiveConfig().getOrcRowIndexCacheTtl().toMillis()),
                    new HiveConfig().getOrcBloomFiltersCacheLimit(),
                    Duration.ofMillis(new HiveConfig().getOrcBloomFiltersCacheTtl().toMillis()),
                    new HiveConfig().getOrcRowDataCacheMaximumWeight(), Duration.ofMillis(new HiveConfig().getOrcRowDataCacheTtl().toMillis()),
                    new HiveConfig().isOrcCacheStatsMetricCollectionEnabled()));
            return createPageSource(pageSourceFactory, session, targetFile, columnNames, columnTypes, HiveStorageFormat.ORC);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
                throws IOException
        {
            return new PrestoOrcFormatWriter(
                    targetFile,
                    columnNames,
                    columnTypes,
                    compressionCodec);
        }
    },

    PRESTO_PARQUET {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new ParquetPageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats(), new HiveConfig().setParquetTimeZone("UTC"));
            return createPageSource(pageSourceFactory, session, targetFile, columnNames, columnTypes, HiveStorageFormat.PARQUET);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.PARQUET, session);
        }
    },

    HIVE_RCBINARY {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HiveRecordCursorProvider cursorProvider = new GenericHiveRecordCursorProvider(hdfsEnvironment);
            return createPageSource(cursorProvider, session, targetFile, columnNames, columnTypes, HiveStorageFormat.RCBINARY);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.RCBINARY, session);
        }
    },

    HIVE_RCTEXT {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HiveRecordCursorProvider cursorProvider = new GenericHiveRecordCursorProvider(hdfsEnvironment);
            return createPageSource(cursorProvider, session, targetFile, columnNames, columnTypes, HiveStorageFormat.RCTEXT);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.RCTEXT, session);
        }
    },

    HIVE_ORC {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HiveRecordCursorProvider cursorProvider = new GenericHiveRecordCursorProvider(hdfsEnvironment);
            return createPageSource(cursorProvider, session, targetFile, columnNames, columnTypes, HiveStorageFormat.ORC);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.ORC, session);
        }
    },

    HIVE_PARQUET {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new ParquetPageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats(), new HiveConfig().setParquetTimeZone("UTC"));
            return createPageSource(pageSourceFactory, session, targetFile, columnNames, columnTypes, HiveStorageFormat.PARQUET);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.PARQUET, session);
        }
    };

    public boolean supportsDate()
    {
        return true;
    }

    public abstract ConnectorPageSource createFileFormatReader(
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes);

    public abstract FormatWriter createFileFormatWriter(
            ConnectorSession session,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes,
            HiveCompressionCodec compressionCodec)
            throws IOException;

    private static final JobConf conf;

    static {
        conf = new JobConf(new Configuration(false));
        conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    }

    public boolean supports(TestData testData)
    {
        return true;
    }

    private static ConnectorPageSource createPageSource(HiveRecordCursorProvider cursorProvider, ConnectorSession session, File targetFile, List<String> columnNames, List<Type> columnTypes, HiveStorageFormat format)
    {
        List<HiveColumnHandle> columnHandles = new ArrayList<>(columnNames.size());
        TypeTranslator typeTranslator = new HiveTypeTranslator();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Type columnType = columnTypes.get(i);
            columnHandles.add(new HiveColumnHandle(columnName, toHiveType(typeTranslator, columnType), columnType.getTypeSignature(), i, REGULAR, Optional.empty()));
        }

        RecordCursor recordCursor = cursorProvider
                .createRecordCursor(
                        conf,
                        session,
                        getPath(targetFile),
                        0,
                        targetFile.length(),
                        targetFile.length(),
                        createSchema(format, columnNames, columnTypes),
                        columnHandles,
                        TupleDomain.all(),
                        TYPE_MANAGER,
                        false,
                        ImmutableMap.of())
                .get();
        return new RecordPageSource(columnTypes, recordCursor);
    }

    private static ConnectorPageSource createPageSource(
            HivePageSourceFactory pageSourceFactory,
            ConnectorSession session,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes,
            HiveStorageFormat format)
    {
        List<HiveColumnHandle> columnHandles = new ArrayList<>(columnNames.size());
        TypeTranslator typeTranslator = new HiveTypeTranslator();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Type columnType = columnTypes.get(i);
            columnHandles.add(new HiveColumnHandle(columnName, toHiveType(typeTranslator, columnType),
                    columnType.getTypeSignature(), i, REGULAR, Optional.empty()));
        }

        return pageSourceFactory
                .createPageSource(
                        conf,
                        session,
                        getPath(targetFile),
                        0,
                        targetFile.length(),
                        targetFile.length(),
                        createSchema(format, columnNames, columnTypes),
                        columnHandles,
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        null,
                        false,
                        targetFile.lastModified())
                .get();
    }

    private static Path getPath(File targetFile)
    {
        try {
            return new Path(targetFile.getCanonicalPath());
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Path Traversal vulnerabilities may exist！");
        }
    }

    private static class RecordFormatWriter
            implements FormatWriter
    {
        private final RecordFileWriter recordWriter;

        public RecordFormatWriter(File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec,
                HiveStorageFormat format,
                ConnectorSession session)
        {
            JobConf config = new JobConf(conf);
            configureCompression(config, compressionCodec);

            recordWriter = new RecordFileWriter(
                    new Path(targetFile.toURI()),
                    columnNames,
                    fromHiveStorageFormat(format),
                    createSchema(format, columnNames, columnTypes),
                    format.getEstimatedWriterSystemMemoryUsage(),
                    config,
                    TYPE_MANAGER,
                    UTC,
                    session);
        }

        @Override
        public void writePage(Page page)
        {
            for (int position = 0; position < page.getPositionCount(); position++) {
                recordWriter.appendRow(page, position);
            }
        }

        @Override
        public void close()
        {
            recordWriter.commit();
        }
    }

    private static Properties createSchema(HiveStorageFormat format, List<String> columnNames, List<Type> columnTypes)
    {
        Properties schema = new Properties();
        TypeTranslator typeTranslator = new HiveTypeTranslator();
        schema.setProperty(SERIALIZATION_LIB, format.getSerDe());
        schema.setProperty(FILE_INPUT_FORMAT, format.getInputFormat());
        schema.setProperty(META_TABLE_COLUMNS, columnNames.stream()
                .collect(joining(",")));
        schema.setProperty(META_TABLE_COLUMN_TYPES, columnTypes.stream()
                .map(type -> toHiveType(typeTranslator, type))
                .map(HiveType::getHiveTypeName)
                .map(HiveTypeName::toString)
                .collect(joining(":")));
        return schema;
    }

    private static class PrestoRcFileFormatWriter
            implements FormatWriter
    {
        private final RcFileWriter writer;

        public PrestoRcFileFormatWriter(File targetFile, List<Type> types, RcFileEncoding encoding, HiveCompressionCodec compressionCodec)
                throws IOException
        {
            writer = new RcFileWriter(
                    new OutputStreamSliceOutput(new FileOutputStream(targetFile)),
                    types,
                    encoding,
                    compressionCodec.getCodec().map(Class::getName),
                    new AircompressorCodecFactory(new HadoopCodecFactory(getClass().getClassLoader())),
                    ImmutableMap.of(),
                    true);
        }

        @Override
        public void writePage(Page page)
                throws IOException
        {
            writer.write(page);
        }

        @Override
        public void close()
                throws IOException
        {
            writer.close();
        }
    }

    private static class PrestoOrcFormatWriter
            implements FormatWriter
    {
        private final OrcWriter writer;

        public PrestoOrcFormatWriter(File targetFile, List<String> columnNames, List<Type> types, HiveCompressionCodec compressionCodec)
                throws IOException
        {
            writer = new OrcWriter(
                    new OutputStreamOrcDataSink(new FileOutputStream(targetFile)),
                    columnNames,
                    types,
                    compressionCodec.getOrcCompressionKind(),
                    new OrcWriterOptions(),
                    false,
                    ImmutableMap.of(),
                    false,
                    BOTH,
                    new OrcWriterStats(), Optional.empty(), Optional.empty());
        }

        @Override
        public void writePage(Page page)
                throws IOException
        {
            writer.write(page);
        }

        @Override
        public void close()
                throws IOException
        {
            writer.close();
        }
    }
}

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
import io.prestosql.plugin.hive.HiveBucketing.BucketingVersion;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.prestosql.plugin.hive.HiveBucketing.BucketingVersion.BUCKETING_V2;
import static io.prestosql.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.prestosql.spi.session.PropertyMetadata.booleanProperty;
import static io.prestosql.spi.session.PropertyMetadata.doubleProperty;
import static io.prestosql.spi.session.PropertyMetadata.enumProperty;
import static io.prestosql.spi.session.PropertyMetadata.integerProperty;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.apache.hadoop.hive.serde.serdeConstants.FIELD_DELIM;

public class HiveTableProperties
{
    @Deprecated
    public static final String EXTERNAL_LOCATION_PROPERTY = "external_location";

    public static final String LOCATION_PROPERTY = "location";
    public static final String IS_EXTERNAL_TABLE = "external";
    public static final String STORAGE_FORMAT_PROPERTY = "format";
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";
    public static final String BUCKETED_BY_PROPERTY = "bucketed_by";
    public static final String BUCKETING_VERSION = "bucketing_version";
    public static final String BUCKET_COUNT_PROPERTY = "bucket_count";
    public static final String SORTED_BY_PROPERTY = "sorted_by";
    public static final String ORC_BLOOM_FILTER_COLUMNS = "orc_bloom_filter_columns";
    public static final String ORC_BLOOM_FILTER_FPP = "orc_bloom_filter_fpp";
    public static final String AVRO_SCHEMA_URL = "avro_schema_url";
    public static final String TEXTFILE_SKIP_HEADER_LINE_COUNT = "textfile_skip_header_line_count";
    public static final String TEXTFILE_SKIP_FOOTER_LINE_COUNT = "textfile_skip_footer_line_count";
    public static final String CSV_SEPARATOR = "csv_separator";
    public static final String CSV_QUOTE = "csv_quote";
    public static final String CSV_ESCAPE = "csv_escape";
    public static final String TRANSACTIONAL = "transactional";

    public static final Set<String> NON_INHERITABLE_PROPERTIES = ImmutableSet.of(EXTERNAL_LOCATION_PROPERTY, IS_EXTERNAL_TABLE, LOCATION_PROPERTY);

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public HiveTableProperties(TypeManager typeManager, HiveConfig config)
    {
        tableProperties = ImmutableList.of(
                stringProperty(
                        EXTERNAL_LOCATION_PROPERTY,
                        format("Deprecated, use '%s' and '%s' table properties instead", LOCATION_PROPERTY, IS_EXTERNAL_TABLE),
                        null,
                        false),
                stringProperty(
                        LOCATION_PROPERTY,
                        "File system location URI for the table",
                        null,
                        false),
                booleanProperty(
                        IS_EXTERNAL_TABLE,
                        "Is the table external Hive table",
                        false,
                        false),
                enumProperty(
                        STORAGE_FORMAT_PROPERTY,
                        "Hive storage format for the table",
                        HiveStorageFormat.class,
                        config.getHiveStorageFormat(),
                        false),
                new PropertyMetadata<>(
                        PARTITIONED_BY_PROPERTY,
                        "Partition columns",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(Collectors.toList())),
                        value -> value),
                new PropertyMetadata<>(
                        BUCKETED_BY_PROPERTY,
                        "Bucketing columns",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(Collectors.toList())),
                        value -> value),
                new PropertyMetadata<>(
                        SORTED_BY_PROPERTY,
                        "Bucket sorting columns",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((Collection<?>) value).stream()
                                .map(String.class::cast)
                                .map(name -> name.toLowerCase(ENGLISH))
                                .map(HiveTableProperties::sortingColumnFromString)
                                .collect(toImmutableList()),
                        value -> ((Collection<?>) value).stream()
                                .map(SortingColumn.class::cast)
                                .map(HiveTableProperties::sortingColumnToString)
                                .collect(toImmutableList())),
                new PropertyMetadata<>(
                        ORC_BLOOM_FILTER_COLUMNS,
                        "ORC Bloom filter index columns",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((Collection<?>) value).stream()
                                .map(String.class::cast)
                                .map(name -> name.toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value),
                doubleProperty(
                        ORC_BLOOM_FILTER_FPP,
                        "ORC Bloom filter false positive probability",
                        config.getOrcDefaultBloomFilterFpp(),
                        false),
                integerProperty(BUCKETING_VERSION, "Bucketing version", null, false),
                integerProperty(BUCKET_COUNT_PROPERTY, "Number of buckets", 0, false),
                stringProperty(AVRO_SCHEMA_URL, "URI pointing to Avro schema for the table", null, false),
                integerProperty(TEXTFILE_SKIP_HEADER_LINE_COUNT, "Number of header lines", null, false),
                integerProperty(TEXTFILE_SKIP_FOOTER_LINE_COUNT, "Number of footer lines", null, false),
                stringProperty(CSV_SEPARATOR, "CSV separator character", null, false),
                stringProperty(CSV_QUOTE, "CSV quote character", null, false),
                stringProperty(CSV_ESCAPE, "CSV escape character", null, false),
                booleanProperty(
                        TRANSACTIONAL,
                        "Is transactional property enabled",
                        false,
                        false),
                stringProperty(
                        FIELD_DELIM,
                        format("Field Delimiter for the table"),
                        null,
                        false));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static String getExternalLocation(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(EXTERNAL_LOCATION_PROPERTY);
    }

    public static Optional<String> getLocation(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(LOCATION_PROPERTY));
    }

    public static boolean isExternalTable(Map<String, Object> tableProperties)
    {
        return Boolean.TRUE.equals(tableProperties.get(IS_EXTERNAL_TABLE));
    }

    public static String getAvroSchemaUrl(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(AVRO_SCHEMA_URL);
    }

    public static Optional<Integer> getTextHeaderSkipCount(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Integer) tableProperties.get(TEXTFILE_SKIP_HEADER_LINE_COUNT));
    }

    public static Optional<Integer> getTextFooterSkipCount(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Integer) tableProperties.get(TEXTFILE_SKIP_FOOTER_LINE_COUNT));
    }

    public static HiveStorageFormat getHiveStorageFormat(Map<String, Object> tableProperties)
    {
        return (HiveStorageFormat) tableProperties.get(STORAGE_FORMAT_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitionedBy(Map<String, Object> tableProperties)
    {
        List<String> partitionedBy = (List<String>) tableProperties.get(PARTITIONED_BY_PROPERTY);
        return partitionedBy == null ? ImmutableList.of() : ImmutableList.copyOf(partitionedBy);
    }

    public static Optional<HiveBucketProperty> getBucketProperty(Map<String, Object> tableProperties)
    {
        List<String> bucketedBy = getBucketedBy(tableProperties);
        List<SortingColumn> sortedBy = getSortedBy(tableProperties);
        int bucketCount = (Integer) tableProperties.get(BUCKET_COUNT_PROPERTY);
        if ((bucketedBy.isEmpty()) && (bucketCount == 0)) {
            if (!sortedBy.isEmpty()) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s may be specified only when %s is specified", SORTED_BY_PROPERTY, BUCKETED_BY_PROPERTY));
            }
            return Optional.empty();
        }
        if (bucketCount < 0) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s must be greater than zero", BUCKET_COUNT_PROPERTY));
        }
        if (bucketedBy.isEmpty() || bucketCount == 0) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s and %s must be specified together", BUCKETED_BY_PROPERTY, BUCKET_COUNT_PROPERTY));
        }
        BucketingVersion bucketingVersion = getBucketingVersion(tableProperties);
        return Optional.of(new HiveBucketProperty(bucketedBy, bucketingVersion, bucketCount, sortedBy));
    }

    public static BucketingVersion getBucketingVersion(Map<String, Object> tableProperties)
    {
        Integer property = (Integer) tableProperties.get(BUCKETING_VERSION);
        if (property == null || property == 1) {
            return BUCKETING_V1;
        }
        if (property == 2) {
            return BUCKETING_V2;
        }
        throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s must be between 1 and 2 (inclusive): %s", BUCKETING_VERSION, property));
    }

    @SuppressWarnings("unchecked")
    private static List<String> getBucketedBy(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.get(BUCKETED_BY_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    private static List<SortingColumn> getSortedBy(Map<String, Object> tableProperties)
    {
        return (List<SortingColumn>) tableProperties.get(SORTED_BY_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getOrcBloomFilterColumns(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.get(ORC_BLOOM_FILTER_COLUMNS);
    }

    public static Double getOrcBloomFilterFpp(Map<String, Object> tableProperties)
    {
        return (Double) tableProperties.get(ORC_BLOOM_FILTER_FPP);
    }

    public static Optional<Character> getCsvProperty(Map<String, Object> tableProperties, String key)
    {
        Object value = tableProperties.get(key);
        if (value == null) {
            return Optional.empty();
        }
        String csvValue = (String) value;
        if (csvValue.length() != 1) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s must be a single character string, but was: '%s'", key, csvValue));
        }
        return Optional.of(csvValue.charAt(0));
    }

    private static SortingColumn sortingColumnFromString(String inputName)
    {
        String name = inputName;
        SortingColumn.Order order = SortingColumn.Order.ASCENDING;
        String lower = name.toUpperCase(ENGLISH);
        if (lower.endsWith(" ASC")) {
            name = name.substring(0, name.length() - 4).trim();
        }
        else if (lower.endsWith(" DESC")) {
            name = name.substring(0, name.length() - 5).trim();
            order = SortingColumn.Order.DESCENDING;
        }
        return new SortingColumn(name, order);
    }

    private static String sortingColumnToString(SortingColumn column)
    {
        return column.getColumnName() + ((column.getOrder() == SortingColumn.Order.DESCENDING) ? " DESC" : "");
    }

    public static boolean getTransactionalValue(Map<String, Object> tableProperties)
    {
        return Boolean.TRUE.equals(tableProperties.get(TRANSACTIONAL));
    }
}

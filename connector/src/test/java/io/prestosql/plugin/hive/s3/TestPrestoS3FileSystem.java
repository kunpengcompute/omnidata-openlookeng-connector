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
package io.prestosql.plugin.hive.s3;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.EncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.VerifyException;
import io.prestosql.plugin.hive.s3.PrestoS3FileSystem.UnrecoverableS3OperationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.testng.SkipException;
import org.testng.annotations.Test;

import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_ACCESS_KEY;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_ACL_TYPE;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_CREDENTIALS_PROVIDER;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_DIRECTORY_OBJECT_CONTENT_TYPE;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_ENCRYPTION_MATERIALS_PROVIDER;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_ENDPOINT;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_IAM_ROLE;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_KMS_KEY_ID;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_MAX_BACKOFF_TIME;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_MAX_CLIENT_RETRIES;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_MAX_RETRY_TIME;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_PATH_STYLE_ACCESS;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_PIN_CLIENT_TO_CURRENT_REGION;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_SECRET_KEY;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_SIGNER_TYPE;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_SKIP_GLACIER_OBJECTS;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_STAGING_DIRECTORY;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_USER_AGENT_PREFIX;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_USER_AGENT_SUFFIX;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_USE_INSTANCE_CREDENTIALS;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.createTempFile;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPrestoS3FileSystem
{
    private static final int HTTP_RANGE_NOT_SATISFIABLE = 416;

    @Test
    public void testStaticCredentials()
            throws Exception
    {
        Configuration config = new Configuration();
        config.set(S3_ACCESS_KEY, "test_secret_access_key");
        config.set(S3_SECRET_KEY, "test_access_key_id");
        // the static credentials should be preferred

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertInstanceOf(getAwsCredentialsProvider(fs), AWSStaticCredentialsProvider.class);
        }
    }

    @Test
    public void testCompatibleStaticCredentials()
            throws Exception
    {
        Configuration config = new Configuration();
        config.set(S3_ACCESS_KEY, "test_secret_access_key");
        config.set(S3_SECRET_KEY, "test_access_key_id");
        config.set(S3_ENDPOINT, "test.example.endpoint.com");
        config.set(S3_SIGNER_TYPE, "S3SignerType");
        // the static credentials should be preferred

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3a://test-bucket/"), config);
            assertInstanceOf(getAwsCredentialsProvider(fs), AWSStaticCredentialsProvider.class);
        }
    }

    @Test(expectedExceptions = VerifyException.class, expectedExceptionsMessageRegExp = "Invalid configuration: either endpoint can be set or S3 client can be pinned to the current region")
    public void testEndpointWithPinToCurrentRegionConfiguration()
            throws Exception
    {
        Configuration config = new Configuration();
        config.set(S3_ENDPOINT, "test.example.endpoint.com");
        config.set(S3_PIN_CLIENT_TO_CURRENT_REGION, "true");
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3a://test-bucket/"), config);
        }
    }

    @Test
    public void testInstanceCredentialsEnabled()
            throws Exception
    {
        Configuration config = new Configuration();
        // instance credentials are enabled by default

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertInstanceOf(getAwsCredentialsProvider(fs), InstanceProfileCredentialsProvider.class);
        }
    }

    @Test
    public void testAssumeRoleCredentials()
            throws Exception
    {
        Configuration config = new Configuration();
        config.set(S3_IAM_ROLE, "role");
        config.setBoolean(S3_USE_INSTANCE_CREDENTIALS, false);

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertInstanceOf(getAwsCredentialsProvider(fs), STSAssumeRoleSessionCredentialsProvider.class);
        }
    }

    @Test
    public void testDefaultCredentials()
            throws Exception
    {
        Configuration config = new Configuration();
        config.setBoolean(S3_USE_INSTANCE_CREDENTIALS, false);

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertInstanceOf(getAwsCredentialsProvider(fs), DefaultAWSCredentialsProviderChain.class);
        }
    }

    @Test
    public void testPathStyleAccess()
            throws Exception
    {
        Configuration config = new Configuration();
        config.setBoolean(S3_PATH_STYLE_ACCESS, true);

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            S3ClientOptions clientOptions = getFieldValue(fs.getS3Client(), AmazonS3Client.class, "clientOptions", S3ClientOptions.class);
            assertTrue(clientOptions.isPathStyleAccess());
        }
    }

    @Test
    public void testUnderscoreBucket()
            throws Exception
    {
        Configuration config = new Configuration();
        config.setBoolean(S3_PATH_STYLE_ACCESS, true);

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            String expectedBucketName = "test-bucket_underscore";
            fs.initialize(new URI("s3n://" + expectedBucketName + "/"), config);
            fs.setS3Client(s3);
            fs.getS3ObjectMetadata(new Path("/test/path"));
            assertEquals(expectedBucketName, s3.getGetObjectMetadataRequest().getBucketName());
        }
    }

    @SuppressWarnings({"ResultOfMethodCallIgnored", "OverlyStrongTypeCast", "ConstantConditions"})
    @Test
    public void testReadRetryCounters()
            throws Exception
    {
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            int maxRetries = 2;
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectHttpErrorCode(HTTP_INTERNAL_ERROR);
            Configuration configuration = new Configuration();
            configuration.set(S3_MAX_BACKOFF_TIME, "1ms");
            configuration.set(S3_MAX_RETRY_TIME, "5s");
            configuration.setInt(S3_MAX_CLIENT_RETRIES, maxRetries);
            fs.initialize(new URI("s3n://test-bucket/"), configuration);
            fs.setS3Client(s3);
            try (FSDataInputStream inputStream = fs.open(new Path("s3n://test-bucket/test"))) {
                inputStream.read();
            }
            catch (Throwable expected) {
                assertInstanceOf(expected, AmazonS3Exception.class);
                assertEquals(((AmazonS3Exception) expected).getStatusCode(), HTTP_INTERNAL_ERROR);
                assertEquals(PrestoS3FileSystem.getFileSystemStats().getReadRetries().getTotalCount(), maxRetries);
                assertEquals(PrestoS3FileSystem.getFileSystemStats().getGetObjectRetries().getTotalCount(), (maxRetries + 1L) * maxRetries);
            }
        }
    }

    @SuppressWarnings({"OverlyStrongTypeCast", "ConstantConditions"})
    @Test
    public void testGetMetadataRetryCounter()
    {
        int maxRetries = 2;
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectMetadataHttpCode(HTTP_INTERNAL_ERROR);
            Configuration configuration = new Configuration();
            configuration.set(S3_MAX_BACKOFF_TIME, "1ms");
            configuration.set(S3_MAX_RETRY_TIME, "5s");
            configuration.setInt(S3_MAX_CLIENT_RETRIES, maxRetries);
            fs.initialize(new URI("s3n://test-bucket/"), configuration);
            fs.setS3Client(s3);
            fs.getS3ObjectMetadata(new Path("s3n://test-bucket/test"));
        }
        catch (Throwable expected) {
            assertInstanceOf(expected, AmazonS3Exception.class);
            assertEquals(((AmazonS3Exception) expected).getStatusCode(), HTTP_INTERNAL_ERROR);
            assertEquals(PrestoS3FileSystem.getFileSystemStats().getGetMetadataRetries().getTotalCount(), maxRetries);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = ".*Failing getObject call with " + HTTP_NOT_FOUND + ".*")
    public void testReadNotFound()
            throws Exception
    {
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectHttpErrorCode(HTTP_NOT_FOUND);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration());
            fs.setS3Client(s3);
            try (FSDataInputStream inputStream = fs.open(new Path("s3n://test-bucket/test"))) {
                inputStream.read();
            }
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = ".*Failing getObject call with " + HTTP_FORBIDDEN + ".*")
    public void testReadForbidden()
            throws Exception
    {
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectHttpErrorCode(HTTP_FORBIDDEN);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration());
            fs.setS3Client(s3);
            try (FSDataInputStream inputStream = fs.open(new Path("s3n://test-bucket/test"))) {
                inputStream.read();
            }
        }
    }

    @Test
    public void testCreateWithNonexistentStagingDirectory()
            throws Exception
    {
        java.nio.file.Path stagingParent = createTempDirectory("test");
        java.nio.file.Path staging = Paths.get(stagingParent.toString(), "staging");

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            Configuration conf = new Configuration();
            conf.set(S3_STAGING_DIRECTORY, staging.toString());
            fs.initialize(new URI("s3n://test-bucket/"), conf);
            fs.setS3Client(s3);
            FSDataOutputStream stream = fs.create(new Path("s3n://test-bucket/test"));
            stream.close();
            assertTrue(Files.exists(staging));
        }
        finally {
            deleteRecursively(stagingParent, ALLOW_INSECURE);
        }
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "Configured staging path is not a directory: .*")
    public void testCreateWithStagingDirectoryFile()
            throws Exception
    {
        java.nio.file.Path staging = createTempFile("staging", null);

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            Configuration conf = new Configuration();
            conf.set(S3_STAGING_DIRECTORY, staging.toString());
            fs.initialize(new URI("s3n://test-bucket/"), conf);
            fs.setS3Client(s3);
            fs.create(new Path("s3n://test-bucket/test"));
        }
        finally {
            Files.deleteIfExists(staging);
        }
    }

    @Test
    public void testCreateWithStagingDirectorySymlink()
            throws Exception
    {
        java.nio.file.Path staging = createTempDirectory("staging");
        java.nio.file.Path link = Paths.get(staging + ".symlink");

        try {
            try {
                Files.createSymbolicLink(link, staging);
            }
            catch (UnsupportedOperationException e) {
                throw new SkipException("Filesystem does not support symlinks", e);
            }

            try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
                MockAmazonS3 s3 = new MockAmazonS3();
                Configuration conf = new Configuration();
                conf.set(S3_STAGING_DIRECTORY, link.toString());
                fs.initialize(new URI("s3n://test-bucket/"), conf);
                fs.setS3Client(s3);
                FSDataOutputStream stream = fs.create(new Path("s3n://test-bucket/test"));
                stream.close();
                assertTrue(Files.exists(link));
            }
        }
        finally {
            deleteRecursively(link, ALLOW_INSECURE);
            deleteRecursively(staging, ALLOW_INSECURE);
        }
    }

    @Test
    public void testReadRequestRangeNotSatisfiable()
            throws Exception
    {
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectHttpErrorCode(HTTP_RANGE_NOT_SATISFIABLE);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration());
            fs.setS3Client(s3);
            try (FSDataInputStream inputStream = fs.open(new Path("s3n://test-bucket/test"))) {
                assertEquals(inputStream.read(), -1);
            }
        }
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = ".*Failing getObjectMetadata call with " + HTTP_FORBIDDEN + ".*")
    public void testGetMetadataForbidden()
            throws Exception
    {
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectMetadataHttpCode(HTTP_FORBIDDEN);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration());
            fs.setS3Client(s3);
            fs.getS3ObjectMetadata(new Path("s3n://test-bucket/test"));
        }
    }

    @Test
    public void testGetMetadataNotFound()
            throws Exception
    {
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectMetadataHttpCode(HTTP_NOT_FOUND);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration());
            fs.setS3Client(s3);
            assertEquals(fs.getS3ObjectMetadata(new Path("s3n://test-bucket/test")), null);
        }
    }

    @Test
    public void testEncryptionMaterialsProvider()
            throws Exception
    {
        Configuration config = new Configuration();
        config.set(S3_ENCRYPTION_MATERIALS_PROVIDER, TestEncryptionMaterialsProvider.class.getName());

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertInstanceOf(fs.getS3Client(), AmazonS3EncryptionClient.class);
        }
    }

    @Test
    public void testKMSEncryptionMaterialsProvider()
            throws Exception
    {
        Configuration config = new Configuration();
        config.set(S3_KMS_KEY_ID, "test-key-id");

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertInstanceOf(fs.getS3Client(), AmazonS3EncryptionClient.class);
        }
    }

    @Test(expectedExceptions = UnrecoverableS3OperationException.class, expectedExceptionsMessageRegExp = ".*\\Q (Path: /tmp/test/path)\\E")
    public void testUnrecoverableS3ExceptionMessage()
            throws Exception
    {
        throw new UnrecoverableS3OperationException(new Path("/tmp/test/path"), new IOException("test io exception"));
    }

    @Test
    public void testCustomCredentialsProvider()
            throws Exception
    {
        Configuration config = new Configuration();
        config.set(S3_USE_INSTANCE_CREDENTIALS, "false");
        config.set(S3_CREDENTIALS_PROVIDER, TestCredentialsProvider.class.getName());
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertInstanceOf(getAwsCredentialsProvider(fs), TestCredentialsProvider.class);
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Error creating an instance of .*")
    public void testCustomCredentialsClassCannotBeFound()
            throws Exception
    {
        Configuration config = new Configuration();
        config.set(S3_USE_INSTANCE_CREDENTIALS, "false");
        config.set(S3_CREDENTIALS_PROVIDER, "com.example.DoesNotExist");
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
        }
    }

    @Test
    public void testUserAgentPrefix()
            throws Exception
    {
        String userAgentPrefix = "agent_prefix";
        Configuration config = new Configuration();
        config.set(S3_USER_AGENT_PREFIX, userAgentPrefix);
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            ClientConfiguration clientConfig = getFieldValue(fs.getS3Client(), AmazonWebServiceClient.class, "clientConfiguration", ClientConfiguration.class);
            assertEquals(clientConfig.getUserAgentSuffix(), S3_USER_AGENT_SUFFIX);
            assertEquals(clientConfig.getUserAgentPrefix(), userAgentPrefix);
        }
    }

    @Test
    public void testDefaultS3ClientConfiguration()
            throws Exception
    {
        HiveS3Config defaults = new HiveS3Config();
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration());
            ClientConfiguration config = getFieldValue(fs.getS3Client(), AmazonWebServiceClient.class, "clientConfiguration", ClientConfiguration.class);
            assertEquals(config.getMaxErrorRetry(), defaults.getS3MaxErrorRetries());
            assertEquals(config.getConnectionTimeout(), defaults.getS3ConnectTimeout().toMillis());
            assertEquals(config.getSocketTimeout(), defaults.getS3SocketTimeout().toMillis());
            assertEquals(config.getMaxConnections(), defaults.getS3MaxConnections());
            assertEquals(config.getUserAgentSuffix(), S3_USER_AGENT_SUFFIX);
            assertEquals(config.getUserAgentPrefix(), "");
        }
    }

    @Test
    public void testSkipGlacierObjectsEnabled()
            throws Exception
    {
        assertSkipGlacierObjects(true);
        assertSkipGlacierObjects(false);
    }

    private static void assertSkipGlacierObjects(boolean skipGlacierObjects)
            throws Exception
    {
        Configuration config = new Configuration();
        config.set(S3_SKIP_GLACIER_OBJECTS, String.valueOf(skipGlacierObjects));

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setHasGlacierObjects(true);
            fs.initialize(new URI("s3n://test-bucket/"), config);
            fs.setS3Client(s3);
            FileStatus[] statuses = fs.listStatus(new Path("s3n://test-bucket/test"));
            assertEquals(statuses.length, skipGlacierObjects ? 1 : 2);
        }
    }

    private static AWSCredentialsProvider getAwsCredentialsProvider(PrestoS3FileSystem fs)
    {
        return getFieldValue(fs.getS3Client(), "awsCredentialsProvider", AWSCredentialsProvider.class);
    }

    private static <T> T getFieldValue(Object instance, String name, Class<T> type)
    {
        return getFieldValue(instance, instance.getClass(), name, type);
    }

    @SuppressWarnings("unchecked")
    private static <T> T getFieldValue(Object instance, Class<?> clazz, String name, Class<T> type)
    {
        try {
            Field field = clazz.getDeclaredField(name);
            checkArgument(field.getType() == type, "expected %s but found %s", type, field.getType());
            field.setAccessible(true);
            return (T) field.get(instance);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static class TestEncryptionMaterialsProvider
            implements EncryptionMaterialsProvider
    {
        private final EncryptionMaterials encryptionMaterials;

        public TestEncryptionMaterialsProvider()
        {
            encryptionMaterials = new EncryptionMaterials(new SecretKeySpec(new byte[] {1, 2, 3}, "AES"));
        }

        @Override
        public void refresh()
        {
        }

        @Override
        public EncryptionMaterials getEncryptionMaterials(Map<String, String> materialsDescription)
        {
            return encryptionMaterials;
        }

        @Override
        public EncryptionMaterials getEncryptionMaterials()
        {
            return encryptionMaterials;
        }
    }

    private static class TestCredentialsProvider
            implements AWSCredentialsProvider
    {
        @SuppressWarnings("UnusedParameters")
        public TestCredentialsProvider(URI uri, Configuration conf) {}

        @Override
        public AWSCredentials getCredentials()
        {
            return null;
        }

        @Override
        public void refresh() {}
    }

    @Test
    public void testDefaultAcl()
            throws Exception
    {
        Configuration config = new Configuration();

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            String expectedBucketName = "test-bucket";
            fs.initialize(new URI("s3n://" + expectedBucketName + "/"), config);
            fs.setS3Client(s3);
            try (FSDataOutputStream stream = fs.create(new Path("s3n://test-bucket/test"))) {
                // initiate an upload by creating a stream & closing it immediately
            }
            assertEquals(CannedAccessControlList.Private, s3.getAcl());
        }
    }

    @Test
    public void testFullBucketOwnerControlAcl()
            throws Exception
    {
        Configuration config = new Configuration();
        config.set(S3_ACL_TYPE, "BUCKET_OWNER_FULL_CONTROL");

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            String expectedBucketName = "test-bucket";
            fs.initialize(new URI("s3n://" + expectedBucketName + "/"), config);
            fs.setS3Client(s3);
            try (FSDataOutputStream stream = fs.create(new Path("s3n://test-bucket/test"))) {
                // initiate an upload by creating a stream & closing it immediately
            }
            assertEquals(CannedAccessControlList.BucketOwnerFullControl, s3.getAcl());
        }
    }

    @Test
    public void testEmptyDirectory()
            throws Exception
    {
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3()
            {
                @Override
                public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest)
                {
                    if (getObjectMetadataRequest.getKey().equals("empty-dir/")) {
                        ObjectMetadata objectMetadata = new ObjectMetadata();
                        objectMetadata.setContentType(S3_DIRECTORY_OBJECT_CONTENT_TYPE);
                        return objectMetadata;
                    }
                    return super.getObjectMetadata(getObjectMetadataRequest);
                }
            };
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration());
            fs.setS3Client(s3);

            FileStatus fileStatus = fs.getFileStatus(new Path("s3n://test-bucket/empty-dir/"));
            assertTrue(fileStatus.isDirectory());

            fileStatus = fs.getFileStatus(new Path("s3n://test-bucket/empty-dir"));
            assertTrue(fileStatus.isDirectory());
        }
    }
}

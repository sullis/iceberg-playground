package io.github.sullis.iceberg.playground;

import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.dynamodb.DynamoDbCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class IcebergTest {

  private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.3.0"))
      .withServices(LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3);

  private static final AwsCredentialsProvider CREDENTIALS_PROVIDER = StaticCredentialsProvider.create(
      AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())
  );

  private static final Region REGION = Region.of(LOCALSTACK.getRegion());

  @BeforeAll
  public static void startLocalstack() {
    LOCALSTACK.start();
  }

  @AfterAll
  public static void stopLocalstack() {
    if (LOCALSTACK != null) {
      LOCALSTACK.stop();
    }
  }

  static Stream<Arguments> awsSdkHttpClients() {
    return Stream.of(
        arguments("apacheHttpClient", ApacheHttpClient.builder().build())
    );
  }

  @ParameterizedTest
  @MethodSource("awsSdkHttpClients")
  public void dynamodDbCatalog(final String sdkHttpClientName, final SdkHttpClient sdkHttpClient) throws Throwable {
    final String catalogName = "catalogName-" + UUID.randomUUID();
    final String path = "path-" + UUID.randomUUID();
    final Namespace namespace = Namespace.of("namespace-" + UUID.randomUUID());
    final AwsProperties awsProperties = new AwsProperties();
    final FileIO fileIo = new InMemoryFileIO();
    try (DynamoDbClient dbClient = createDynamoDbClient(sdkHttpClient)) {
      assertThat(dbClient).isNotNull();
      try (DynamoDbCatalog catalog = new DynamoDbCatalog()) {
        Method initialize = catalog.getClass()
            .getDeclaredMethod("initialize", String.class, String.class, AwsProperties.class, DynamoDbClient.class,
                FileIO.class);
        initialize.setAccessible(true);
        initialize.invoke(catalog, catalogName, path, awsProperties, dbClient, fileIo);
        catalog.createNamespace(namespace);
        List<TableIdentifier> listTablesResult = catalog.listTables(namespace);
        assertThat(listTablesResult).isEmpty();
        final String tableName = "tableName-" + UUID.randomUUID();
        final TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
        final Schema schema = new Schema();
        final Table table = catalog.createTable(tableIdentifier, schema);
        assertThat(table.name()).isNotNull();
        assertThat(table.location()).startsWith(path + "/");
      }
    }
  }

  private DynamoDbClient createDynamoDbClient(final SdkHttpClient sdkHttpClient) {
    return DynamoDbClient.builder()
        .httpClient(sdkHttpClient)
        .endpointOverride(LOCALSTACK.getEndpoint())
        .credentialsProvider(CREDENTIALS_PROVIDER)
        .region(REGION)
        .build();
  }
}

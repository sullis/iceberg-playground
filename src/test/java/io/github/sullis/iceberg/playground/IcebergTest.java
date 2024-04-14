package io.github.sullis.iceberg.playground;

import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;
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
import org.junit.jupiter.api.Test;
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

public class IcebergTest {
  private static final SdkHttpClient.Builder<?> AWS_SDK_HTTP_CLIENT_BUILDER = ApacheHttpClient.builder();

  private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.3.0"))
      .withServices(LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3);

  private static final AwsCredentialsProvider AWS_CREDENTIALS_PROVIDER = StaticCredentialsProvider.create(
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

  @Test
  public void dynamodDbCatalog() throws Throwable {
    final String catalogName = "catalogName-" + UUID.randomUUID();
    final String path = "path-" + UUID.randomUUID();
    final Namespace namespace = Namespace.of("namespace-" + UUID.randomUUID());
    final AwsProperties awsProperties = new AwsProperties();
    final FileIO fileIo = new InMemoryFileIO();
    try (DynamoDbClient dbClient = createDynamoDbClient()) {
      assertThat(dbClient).isNotNull();
      try (DynamoDbCatalog catalog = createDynamoDbCatalog(catalogName, path, dbClient, awsProperties, fileIo)) {
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

  private DynamoDbCatalog createDynamoDbCatalog(String catalogName, String path, DynamoDbClient dbClient, AwsProperties awsProperties, FileIO fileIo) throws Exception {
    DynamoDbCatalog catalog = new DynamoDbCatalog();
    Method initializeMethod = catalog.getClass()
        .getDeclaredMethod("initialize", String.class, String.class, AwsProperties.class, DynamoDbClient.class,
            FileIO.class);
    initializeMethod.setAccessible(true);
    initializeMethod.invoke(catalog, catalogName, path, awsProperties, dbClient, fileIo);
    return catalog;
  }

  private DynamoDbClient createDynamoDbClient() {
    return DynamoDbClient.builder()
        .httpClient(AWS_SDK_HTTP_CLIENT_BUILDER.build())
        .endpointOverride(LOCALSTACK.getEndpoint())
        .credentialsProvider(AWS_CREDENTIALS_PROVIDER)
        .region(REGION)
        .build();
  }
}

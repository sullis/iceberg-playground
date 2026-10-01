package io.github.sullis.iceberg.playground;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import org.apache.iceberg.catalog.Catalog;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Object;


public class LocalIcebergCatalog {
  private static final Region AWS_REGION = Region.US_EAST_1;
  private static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:4.3.0");
  private final String s3BucketName = "test-bucket";
  private final String warehouseLocation = "s3://" + s3BucketName + "/iceberg";
  private File localDir;
  private File s3DataDir;
  private File h2Dir;
  private LocalStackContainer localstack;
  private Catalog catalog;
  private final Map<String, String> extraCatalogProperties;
  private final AtomicReference<Status> status = new AtomicReference<>(Status.STOPPED);

  enum Status {
      STOPPED,
      STARTING,
      STARTED
  }

  public LocalIcebergCatalog() {
      this(createTempDirectory(), new HashMap<>());
  }

  public LocalIcebergCatalog(final Map<String, String> extraCatalogProps) {
    this(createTempDirectory(), extraCatalogProps);
  }

  public LocalIcebergCatalog(final File localDir) {
    this(localDir, new HashMap<>());
  }

  public LocalIcebergCatalog(final File localDir, final Map<String, String> extraCatalogProps) {
    this.localDir = localDir;
    this.localDir.mkdirs();
    this.s3DataDir = new File(this.localDir, "s3-data");
    this.s3DataDir.mkdirs();
    this.h2Dir = new File(this.localDir, "h2db-data");
    this.h2Dir.mkdirs();
    this.extraCatalogProperties = extraCatalogProps;
  }

  private static File createTempDirectory() {
    try {
      return Files.createTempDirectory("iceberg-local").toFile();
    } catch (IOException ex) {
      throw new IllegalStateException("unable to create localDir");
    }
  }

  public File getLocalDirectory() {
    return localDir;
  }

  public String getS3BucketName() {
    return s3BucketName;
  }

  public String getS3Endpoint() {
    return localstack.getEndpoint().toString();
  }

  public S3Client createS3Client() {
    final URI uri = localstack.getEndpoint();
    return S3Client.builder()
      .region(AWS_REGION)
      .credentialsProvider(
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
      .applyMutation(mutator -> mutator.endpointOverride(uri))
      .forcePathStyle(true) // OSX won't resolve subdomains
      .build();
  }

  public void start() {
    if (!this.status.compareAndSet(Status.STOPPED, Status.STARTING)) {
      throw new IllegalStateException("Cannot start. status=" + this.status.get());
    }

    if (localstack == null) {
      localstack = new LocalStackContainer(LOCALSTACK_IMAGE);
      localstack.withEnv("SERVICES", "s3");
    }

    localstack.start();

    try (S3Client s3 = createS3Client()) {
      try {
        s3.headBucket(builder -> builder.bucket(s3BucketName));
      } catch (NoSuchBucketException ex) {
        s3.createBucket(builder -> builder.bucket(s3BucketName));
      }
    }

    // LocalStack community keeps object storage in memory only, so the bucket
    // contents are reloaded from localDir on every start.
    restoreBucketFromLocalDir();

    Map<String, String> props = new HashMap<>();
    props.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
    props.put(CatalogProperties.URI, this.getJdbcUrl());
    props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    props.put(S3FileIOProperties.ACCESS_KEY_ID, localstack.getAccessKey());
    props.put(S3FileIOProperties.SECRET_ACCESS_KEY, localstack.getSecretKey());
    props.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
    props.put(S3FileIOProperties.ENDPOINT, this.getS3Endpoint());
    props.put(AwsClientProperties.CLIENT_REGION, AWS_REGION.id());
    if (this.extraCatalogProperties != null) {
      props.putAll(this.extraCatalogProperties);
    }

    JdbcCatalog jdbc = new JdbcCatalog();
    jdbc.initialize("jdbccatalog", props);
    this.catalog = jdbc;

    if (!this.status.compareAndSet(Status.STARTING, Status.STARTED)) {
      throw new IllegalStateException("unable to complete start()");
    }
  }

  public void stop() {
    if (isStopped()) {
      return;
    }

    if (localstack != null) {
      saveBucketToLocalDir();
      localstack.stop();
      localstack = null;
    }
    this.status.set(Status.STOPPED);
  }

  /** Copies every object in the bucket into localDir so it survives the container. */
  private void saveBucketToLocalDir() {
    final Path root = s3DataDir.toPath();
    try (S3Client s3 = createS3Client()) {
      deleteRecursively(root);
      Files.createDirectories(root);
      ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(s3BucketName).build();
      for (S3Object object : s3.listObjectsV2Paginator(request).contents()) {
        Path destination = root.resolve(object.key());
        Files.createDirectories(destination.getParent());
        s3.getObject(
            builder -> builder.bucket(s3BucketName).key(object.key()),
            ResponseTransformer.toFile(destination));
      }
    } catch (IOException ex) {
      throw new UncheckedIOException("unable to save bucket to " + root, ex);
    }
  }

  /** Repopulates the bucket from the objects previously written into localDir. */
  private void restoreBucketFromLocalDir() {
    final Path root = s3DataDir.toPath();
    if (!Files.isDirectory(root)) {
      return;
    }
    try (S3Client s3 = createS3Client();
        Stream<Path> files = Files.walk(root)) {
      files.filter(Files::isRegularFile).forEach(file ->
          s3.putObject(
              builder -> builder.bucket(s3BucketName).key(root.relativize(file).toString()),
              RequestBody.fromFile(file)));
    } catch (IOException ex) {
      throw new UncheckedIOException("unable to restore bucket from " + root, ex);
    }
  }

  private static void deleteRecursively(final Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(root)) {
      for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
        Files.delete(path);
      }
    }
  }

  public boolean isStopped() {
    return this.status.get() == Status.STOPPED;
  }

  public String getWarehouseLocation() {
    return this.warehouseLocation;
  }

  public Catalog getCatalog() {
    return catalog;
  }

  public String getJdbcUrl() {
    return "jdbc:h2:" + this.h2Dir.getAbsolutePath() + ";DATABASE_TO_UPPER=FALSE";
  }

  @Override
  public String toString() {
    return this.getClass().getName() + " " + this.status;
  }
}

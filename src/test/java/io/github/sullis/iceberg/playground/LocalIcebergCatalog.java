package io.github.sullis.iceberg.playground;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.MinIOContainer;
import org.apache.iceberg.catalog.Catalog;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;


public class LocalIcebergCatalog {
  private static final Region REGION = Region.US_EAST_1;
  private final String s3BucketName = "test-bucket";
  private final String warehouseLocation = "s3://" + s3BucketName + "/warehouse";
  private File localDir;
  private File minioDataDir;
  private MinIOContainer minio;
  private Catalog catalog;
  private final AtomicReference<Status> status = new AtomicReference<>(Status.STOPPED);

  enum Status {
      STOPPED,
      STARTING,
      STARTED
  }

  public LocalIcebergCatalog() {
      this(createTempDirectory());
  }

  public LocalIcebergCatalog(final File localDir) {
    this.localDir = localDir;
    this.localDir.mkdirs();
    this.minioDataDir = new File(this.localDir, "minio-data");
    this.minioDataDir.mkdirs();
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

  public S3Client createS3Client() {
    final URI uri = URI.create(minio.getS3URL());
    return S3Client.builder()
      .region(REGION)
      .credentialsProvider(
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(minio.getUserName(), minio.getPassword())))
      .applyMutation(mutator -> mutator.endpointOverride(uri))
      .forcePathStyle(true) // OSX won't resolve subdomains
      .build();
  }

  public void start() {
    if (!this.status.compareAndSet(Status.STOPPED, Status.STARTING)) {
      throw new IllegalStateException("server is not stopped");
    }

    if (minio == null) {
      minio = new MinIOContainer("minio/minio:latest");
      minio.withFileSystemBind(minioDataDir.getAbsolutePath(), "/data",  BindMode.READ_WRITE);
      minio.withEnv("MINIO_DOMAIN", "localhost");
    }

    if (!minio.isRunning()) {
      minio.start();
    }

    try (S3Client s3 = createS3Client()) {
      try {
        s3.headBucket(builder -> builder.bucket(s3BucketName));
      } catch (NoSuchBucketException ex) {
        s3.createBucket(builder -> builder.bucket(s3BucketName));
      }
    }

    JdbcCatalog jdbc = new JdbcCatalog();
    jdbc.initialize("jdbccatalog",
        Map.of(
          CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName(),
          CatalogProperties.URI, this.getJdbcUrl(),
          CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation,
          S3FileIOProperties.ACCESS_KEY_ID, this.minio.getUserName(),
          S3FileIOProperties.SECRET_ACCESS_KEY, this.minio.getPassword(),
          S3FileIOProperties.PATH_STYLE_ACCESS, "true",
          S3FileIOProperties.ENDPOINT, this.minio.getS3URL(),
          AwsClientProperties.CLIENT_REGION, REGION.id()
    ));
    this.catalog = jdbc;
    if (!this.status.compareAndSet(Status.STARTING, Status.STARTED)) {
      throw new IllegalStateException("unable to complete start()");
    }
  }

  public void stop() {
    if (minio != null) {
      minio.stop();
    }
    this.status.set(Status.STOPPED);
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

  private String getH2Dir() {
      File f = new File(this.localDir, ".h2db");
      f.mkdirs();
      return f.getAbsolutePath();
  }

  public String getJdbcUrl() {
    return "jdbc:h2:" + getH2Dir() + ";DATABASE_TO_UPPER=FALSE";
  }
}

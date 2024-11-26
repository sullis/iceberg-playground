package io.github.sullis.iceberg.playground;

import java.io.File;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.Test;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

public class LocalIcebergCatalogTest {

  @Test
  public void happyPath() throws Exception {

    LocalIcebergCatalog localCatalog = new LocalIcebergCatalog();
    assertThat(localCatalog.getLocalDirectory()).exists().isDirectory();

    localCatalog.start();

    final Namespace namespace = Namespace.of("mynamespace");
    final TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "mytable" + UUID.randomUUID());

    final var columns = List.of(
        required(1, "text", Types.StringType.get()),
        required(2, "count", Types.IntegerType.get()),
        required(3, "amazing", Types.BooleanType.get()),
        required(4, "event_timestamp", Types.TimestampType.withZone())
        );

    final Schema schema = new Schema(columns);

    final PartitionSpec spec = PartitionSpec.builderFor(schema).build();

    {
      Catalog catalog = localCatalog.getCatalog();
      Table t = catalog.createTable(tableIdentifier, schema);
      Table loaded = catalog.loadTable(tableIdentifier);
      assertThat(t).isNotNull();
      assertThat(loaded).isNotNull();
      assertThat(t.location())
          .isEqualTo(loaded.location());
      assertThat(t.schema().schemaId())
          .isEqualTo(loaded.schema().schemaId());
      assertThat(t.schema().sameSchema(loaded.schema()))
          .isTrue();

      FileIO io = loaded.io();
      OutputFile outputFile = io.newOutputFile(loaded.location() + "/foobar/" + System.currentTimeMillis());
      var outputStream = outputFile.create();

      FileAppender appender = Parquet.write(outputFile).forTable(loaded).build();

      AppendFiles appendFiles = loaded.newAppend();

      appendFiles.appendFile(DataFiles.builder(spec)
          .withInputFile(outputFile.toInputFile())
          .build());
    }

    localCatalog.stop();
    assertThat(localCatalog.isStopped()).isTrue();

    for (int i = 0; i < 2; i++) {
      localCatalog.start();
      assertThat(localCatalog.isStopped()).isFalse();

      File localDir = localCatalog.getLocalDirectory();
      assertThat(localDir).exists().isDirectory();

      {
        Catalog catalog = localCatalog.getCatalog();
        Table loaded = catalog.loadTable(tableIdentifier);
        assertThat(loaded).isNotNull();
        assertThat(loaded.io()).isInstanceOf(S3FileIO.class);
        assertThat(loaded.location()).startsWith("s3://test-bucket/warehouse/mynamespace/mytable");
      }

      localCatalog.stop();

      localCatalog = new LocalIcebergCatalog(localDir);
      localCatalog.start();
      {
        Catalog catalog = localCatalog.getCatalog();
        Table loaded = catalog.loadTable(tableIdentifier);
        assertThat(loaded).isNotNull();
        assertThat(loaded.io()).isInstanceOf(S3FileIO.class);
        assertThat(loaded.location()).startsWith("s3://test-bucket/warehouse/mynamespace/mytable");
        assertThat(loaded.schema().sameSchema(schema))
            .isTrue();
      }
      localCatalog.stop();
    }
  }
}

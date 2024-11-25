package io.github.sullis.iceberg.playground;

import java.io.File;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

public class LocalIcebergCatalogTest {

  @Test
  public void happyPath() {
    LocalIcebergCatalog localCatalog = new LocalIcebergCatalog();
    assertThat(localCatalog.getLocalDirectory()).exists().isDirectory();

    localCatalog.start();

    final Namespace namespace = Namespace.of("mynamespace");
    final TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "mytable" + UUID.randomUUID());
    final var columns = List.of(
        required(1, "c1", Types.StringType.get()),
        required(2, "c2", Types.IntegerType.get()),
        required(3, "c3", Types.BooleanType.get())
        );
    final Schema schema = new Schema(columns);

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
        assertThat(loaded.schema().columns().stream().map(f -> f.name()))
            .containsExactly("c1", "c2", "c3");
      }
      localCatalog.stop();
    }
  }
}

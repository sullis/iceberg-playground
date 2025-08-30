package io.github.sullis.iceberg.playground;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.apache.iceberg.types.Types.NestedField.required;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryCatalogTest {

  private InMemoryCatalog catalog;

  @Test
  public void happyPath()
      throws IOException {
    final String tableName = "foobar";
    Namespace namespace = Namespace.of("test" + System.currentTimeMillis());
    String catalogName = "catname" + System.currentTimeMillis();
    catalog = new InMemoryCatalog();
    catalog.initialize(catalogName, new HashMap<>());
    catalog.createNamespace(namespace);
    List<TableIdentifier> listTablesResult = catalog.listTables(namespace);
    assertThat(listTablesResult).isEmpty();

    TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
    List<Types.NestedField> columns = List.of(required(-1, "c1", Types.StringType.get()));
    final Schema schema = new Schema(columns);
    Table table = catalog.createTable(tableId, schema);
    assertThat(catalog.listTables(namespace)).hasSize(1);

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("c1").build();

    DataFile fileA = DataFiles.builder(spec).withPath("/path/to/data-a.parquet").withFileSizeInBytes(10)
        .withPartitionPath("c1=0") // easy way to set partition data for now
        .withRecordCount(1)
        .build();

    DataFile fileB = DataFiles.builder(spec).withPath("/path/to/data-b.parquet").withFileSizeInBytes(10)
        .withPartitionPath("c1=1") // easy way to set partition data for now
        .withRecordCount(1)
        .build();

    AppendFiles appendFiles = table.newFastAppend();
    appendFiles.appendFile(fileA);
    appendFiles.appendFile(fileB);
    appendFiles.commit();

    assertThat(catalog.namespaceExists(namespace)).isTrue();
    assertThat(catalog.listNamespaces()).hasSize(1);

    Table loadTableResult = catalog.loadTable(tableId);
    FileIO io = loadTableResult.io();
    assertThat(io).isInstanceOf(InMemoryFileIO.class);
    String path = "test-path-" + UUID.randomUUID();
    OutputFile outputFile = io.newOutputFile(path);
    PositionOutputStream pos = outputFile.create();
    pos.write("Hello".getBytes(StandardCharsets.UTF_8));
    pos.flush();
    pos.close();
    InputFile inputFile = outputFile.toInputFile();
    assertThat(inputFile.exists()).isTrue();
    SeekableInputStream inputStream = inputFile.newStream();
    assertThat(inputStream).asString(StandardCharsets.UTF_8).isEqualTo("Hello");
    inputStream.close();

    BaseTable baseTable = (BaseTable) catalog.loadTable(tableId);
    TableMetadata metadata = baseTable.operations().current();
    assertThat(metadata.location())
        .endsWith("/" + tableName);
    assertThat(metadata.metadataFileLocation())
        .contains("/" + tableName + "/metadata/")
        .endsWith(".metadata.json");
    assertThat(metadata.properties())
        .containsExactly(Map.entry("write.parquet.compression-codec", "zstd"));
    assertThat(metadata.schemasById())
        .containsKeys(0);
    assertThat(metadata.schemas()).hasSize(1);
    assertThat(metadata.schema().columns()).hasSize(1);

    FileIO baseIO = baseTable.io();
    InputFile metadataFile = baseIO.newInputFile(metadata.metadataFileLocation());

    assertThat(metadataFile.exists()).isTrue();

    assertThat(metadataFile.location())
            .startsWith("/test")
            .contains("/foobar/metadata")
            .endsWith(".metadata.json");

    String metadataContent = new String(metadataFile.newStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(metadataContent)
        .startsWith("{")
        .endsWith("}");
    assertThatJson(metadataContent)
        .isObject()
        .containsKeys(
            "current-schema-id",
            "current-snapshot-id",
            "default-sort-order-id",
            "default-spec-id",
            "format-version",
            "last-column-id",
            "last-partition-id",
            "last-sequence-number",
            "last-updated-ms",
            "location",
            "metadata-log",
            "partition-specs",
            "partition-statistics",
            "properties");
  }

}

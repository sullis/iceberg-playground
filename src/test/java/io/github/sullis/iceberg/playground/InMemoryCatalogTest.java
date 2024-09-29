package io.github.sullis.iceberg.playground;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
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

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryCatalogTest {

  private InMemoryCatalog catalog;

  @Test
  public void happyPath()
      throws IOException {
    Namespace namespace = Namespace.of("test" + System.currentTimeMillis());
    String catalogName = "catname" + System.currentTimeMillis();
    catalog = new InMemoryCatalog();
    catalog.initialize(catalogName, new HashMap<>());
    catalog.createNamespace(namespace);
    var listTablesResult = catalog.listTables(namespace);
    assertThat(listTablesResult).isEmpty();
    TableIdentifier tableId = TableIdentifier.of(namespace, "foobar");
    var columns = List.of(Types.NestedField.of(-1, false, "c1", Types.StringType.get(), "doc"));
    Schema schema = new Schema(columns);
    Table table = catalog.createTable(tableId, schema);
    assertThat(catalog.listTables(namespace)).hasSize(1);

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("c1").build();

    DataFile fileA = DataFiles.builder(spec)
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("c1=0") // easy way to set partition data for now
            .withRecordCount(1)
            .build();
    table.newFastAppend()
        .appendFile(fileA)
        .commit();

    assertThat(catalog.namespaceExists(namespace)).isTrue();
    assertThat(catalog.listNamespaces()).hasSize(1);

    Table loadTableResult = catalog.loadTable(tableId);
    FileIO io = loadTableResult.io();
    assertThat(io).isInstanceOf(InMemoryFileIO.class);
    String outputPath = "test-path-" + UUID.randomUUID();
    OutputFile outputFile = io.newOutputFile(outputPath);
    PositionOutputStream pos = outputFile.create();
    pos.write("Hello".getBytes(StandardCharsets.UTF_8));
    pos.flush();
    pos.close();
    InputFile inputFile = outputFile.toInputFile();
    assertThat(inputFile.exists()).isTrue();
    SeekableInputStream inputStream = inputFile.newStream();
    assertThat(inputStream).asString(StandardCharsets.UTF_8)
        .isEqualTo("Hello");
  }
}

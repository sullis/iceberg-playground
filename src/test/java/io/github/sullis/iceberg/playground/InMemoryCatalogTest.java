package io.github.sullis.iceberg.playground;

import java.util.HashMap;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class InMemoryCatalogTest {

  private InMemoryCatalog catalog;

  @Test
  public void happyPath() {
    Namespace namespace = Namespace.of("test" + System.currentTimeMillis());
    String catalogName = "catname" + System.currentTimeMillis();
    catalog = new InMemoryCatalog();
    catalog.initialize(catalogName, new HashMap<>());
    catalog.createNamespace(namespace);
    var listTablesResult = catalog.listTables(namespace);
    assertThat(listTablesResult).isEmpty();
    TableIdentifier tableId = TableIdentifier.of(namespace, "foobar");
    var columns = List.of(Types.NestedField.of(-1, false, "field1", Types.StringType.get(), "doc"));
    Schema schema = new Schema(columns);
    Table table = catalog.createTable(tableId, schema);
    assertThat(catalog.listTables(namespace)).hasSize(1);
  }
}

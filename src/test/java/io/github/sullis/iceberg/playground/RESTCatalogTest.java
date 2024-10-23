package io.github.sullis.iceberg.playground;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class RESTCatalogTest {

  @Test
  public void restCatalog() throws Exception {
    final String columnName = "c1";
    final String catalogName = "catname" + System.currentTimeMillis();

    Map<String, String> properties = new HashMap<>();

    properties.put(CatalogProperties.CATALOG_IMPL, RESTCatalog.class.getName());
    properties.put(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName());
    properties.put(CatalogProperties.URI, "http://localhost:8181");

    Namespace namespace = Namespace.of("test" + System.currentTimeMillis());

    TableIdentifier tableId = TableIdentifier.of(namespace, "foobar");
    var columns = List.of(Types.NestedField.of(-1, false, columnName, Types.StringType.get(), "doc"));
    Schema schema = new Schema(columns);

    try (RESTCatalog restCatalog = new RESTCatalog()) {
      restCatalog.initialize(catalogName, properties);
      Table table = restCatalog.createTable(tableId, schema);
      assertThat(table).isNotNull();
    }
  }
}

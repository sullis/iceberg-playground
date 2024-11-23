package io.github.sullis.iceberg.playground;

import org.junit.jupiter.api.Test;


public class LocalIcebergRestCatalogTest {
  @Test
  void happyPath() {
    LocalIcebergRestCatalog localCatalog = new LocalIcebergRestCatalog();
    localCatalog.start();
    localCatalog.stop();
  }
}

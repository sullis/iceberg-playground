package io.github.sullis.iceberg.playground;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class LocalIcebergRestCatalogTest {
  @Test
  void happyPath() {
    LocalIcebergRestCatalog local = new LocalIcebergRestCatalog();
    local.start();

    assertThat(local.getRESTCatalog()).isNotNull();

    local.stop();
  }
}

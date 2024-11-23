package io.github.sullis.iceberg.playground;

import java.util.Collections;
import org.apache.iceberg.catalog.Namespace;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class LocalIcebergRestCatalogTest {
  @Test
  void happyPath() {
    LocalIcebergRestCatalog local = new LocalIcebergRestCatalog();
    local.start();

    var restCatalog = local.getRESTCatalog();

    assertThat(restCatalog).isNotNull();

    Namespace ns = Namespace.of("hello");
    // restCatalog.createNamespace(ns, Collections.emptyMap());

    local.stop();
  }
}

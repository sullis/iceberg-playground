package io.github.sullis.iceberg.playground;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalog;
import java.util.Map;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.rest.responses.ConfigResponse;


public class LocalIcebergRestCatalog {
  private RESTCatalog restCatalog;
  private com.sun.net.httpserver.HttpServer httpServer;
  private static final ObjectMapper MAPPER = initObjectMapper();

  private static ObjectMapper initObjectMapper() {
    ObjectMapper om = new ObjectMapper();
    RESTSerializers.registerAll(om);
    return om;
  }

  public LocalIcebergRestCatalog() {

    try {
      httpServer = HttpServer.create(new InetSocketAddress(0), 10);
      httpServer.createContext("/v1/config").setHandler(new HttpHandler() {
        @Override
        public void handle(HttpExchange exchange)
            throws IOException {
          exchange.sendResponseHeaders(200, 0);
          ConfigResponse configResponse = ConfigResponse.builder()
              .withDefaults(ImmutableMap.of(CatalogProperties.CLIENT_POOL_SIZE, "1"))
              .withOverrides(
                  ImmutableMap.of(
                      CatalogProperties.CACHE_ENABLED,
                      "false",
                      CatalogProperties.WAREHOUSE_LOCATION,
                      "todo:warehouse"))
              .build();
          String json = MAPPER.writeValueAsString(configResponse);
          OutputStream out = exchange.getResponseBody();
          out.write(json.getBytes(StandardCharsets.UTF_8));
          out.flush();
          out.close();
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    httpServer.start();
    restCatalog = new RESTCatalog();
    restCatalog.initialize("restCatalog",
        Map.of(CatalogProperties.URI, "http://localhost:" + httpServer.getAddress().getPort())
    );
  }

  public RESTCatalog getRESTCatalog() {
    return this.restCatalog;
  }

  public void start() {
    // todo
  }

  public void stop() {
    // todo
  }
}

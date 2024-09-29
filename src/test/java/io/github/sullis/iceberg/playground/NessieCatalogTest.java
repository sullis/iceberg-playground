package io.github.sullis.iceberg.playground;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.minio.MinioExtension;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;


@ExtendWith(MinioExtension.class)
public class NessieCatalogTest {
  private static final NessieContainer NESSIE_CONTAINER = new NessieContainer(DockerImageName.parse("ghcr.io/projectnessie/nessie"));

  @BeforeAll
  public static void startContainers() {
    NESSIE_CONTAINER.start();
  }

  @AfterAll
  public static void stopContainers() {
    if (NESSIE_CONTAINER != null) {
      NESSIE_CONTAINER.stop();
    }
  }

  @Test
  public void validate() {
    assertThat(NESSIE_CONTAINER.isRunning()).isTrue();
    // todo assertThat(NESSIE_CONTAINER.isHealthy()).isTrue();
  }
}

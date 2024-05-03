package io.github.sullis.iceberg.playground;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;


public class NessieContainer extends GenericContainer {
  public NessieContainer(DockerImageName imageName) {
    super(imageName);
  }
}

package io.github.sullis.iceberg.playground;

import org.apache.iceberg.IcebergBuild;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class IcebergBuildTest {
    @Test
    public void testBuild() {
        assertThat(IcebergBuild.fullVersion())
                .startsWith("Apache Iceberg ")
                .contains("(commit ");
        assertThat(IcebergBuild.version())
                .startsWith("1.");
    }
}

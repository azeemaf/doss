package doss.local;

import java.io.IOException;
import java.nio.file.Path;

interface ContainerType {
    String name();

    Container create(Path areaRoot, long containerId) throws IOException;

    Container openForReading(Path areaRoot, long containerId) throws IOException;

    Container openForWriting(Path areaRoot, long containerId) throws IOException;
}

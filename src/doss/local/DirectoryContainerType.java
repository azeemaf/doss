package doss.local;

import java.io.IOException;
import java.nio.file.Path;

class DirectoryContainerType implements ContainerType {

    @Override
    public String name() {
        return "directory";
    }

    @Override
    public Container create(Path areaRoot, long containerId) throws IOException {
        return openForWriting(areaRoot, containerId);
    }

    @Override
    public Container openForReading(Path areaRoot, long containerId) throws IOException {
        return openForWriting(areaRoot, containerId);
    }

    @Override
    public Container openForWriting(Path areaRoot, long containerId) throws IOException {
        return new DirectoryContainer(containerId, areaRoot.resolve(Long.toString(containerId)));
    }

}

package doss.local;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class TarContainerType implements ContainerType {

    @Override
    public String name() {
        return "tar";
    }

    Path tarPath(Path areaRoot, long containerId) {
        Path path = areaRoot;
        String dirs = "";
        for (long x = containerId / 1000; x > 0; x = x / 1000) {
            dirs = String.format("%03d/%s", x % 1000, dirs);
        }
        return path.resolve(dirs).resolve(String.format("nla.doss-%d.tar", containerId));
    }

    public TarContainer open(Path areaRoot, long containerId, OpenOption... options)
            throws IOException {
        Path path = tarPath(areaRoot, containerId);
        FileChannel channel = FileChannel.open(path, options);
        return new TarContainer(containerId, path, channel);
    }

    @Override
    public TarContainer create(Path areaRoot, long containerId) throws IOException {
        return open(areaRoot, containerId, StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
    }

    @Override
    public TarContainer openForReading(Path areaRoot, long containerId) throws IOException {
        return open(areaRoot, containerId, StandardOpenOption.READ);
    }

    @Override
    public TarContainer openForWriting(Path areaRoot, long containerId) throws IOException {
        return open(areaRoot, containerId, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

}

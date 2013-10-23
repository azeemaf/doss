package doss.local;

import java.nio.file.Path;

public class Filesystem {
    private final String name;
    private final Path path;

    public Filesystem(String name, Path path) {
        this.name = name;
        this.path = path;
    }

    public Path path() {
        return path;
    }
}

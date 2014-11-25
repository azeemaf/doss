package doss.local;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.ini4j.Ini;
import org.ini4j.Profile.Section;

/**
 * Example config file:
 *
 * <pre>
 * [area.staging]
 * fs=staging
 *
 * [fs.staging]
 * path = /staging
 * </pre>
 */
class Config {
    private final Ini ini;
    Path stagingRoot = null;
    List<Path> masterRoots = new ArrayList<>();

    Config(Path path) throws IOException {
        ini = new Ini(path.toFile());
        for (String section : ini.keySet()) {
            if (section.startsWith("area.")) {
                parseArea(section);
            }
        }
        if (stagingRoot == null) {
            barf("at least the staging area and fs must be configured");
        }
    }

    private void parseArea(String name) throws IOException {
        Section section = ini.get(name);
        for (Entry<String, String> entry : section.entrySet()) {
            switch (entry.getKey()) {
            case "fs": {
                for (String shortName : entry.getValue().split(",\\s*")) {
                    String fsName = "fs." + shortName;
                    Section fsSection = ini.get(fsName);
                    if (fsSection == null) {
                        barf("missing " + fsName + " referred to by " + name
                                + "/fs");
                    }
                    Path fsRoot = parseFilesystem(fsSection);
                    if (name.equals("area.staging")) {
                        stagingRoot = fsRoot;
                    } else if (name.equals("area.master")) {
                        masterRoots.add(fsRoot);
                    } else {
                        barf("unknown area " + name + " expected area.master or area.staging");
                    }
                }
                break;
            }
            default:
                barf("unknown option: " + name + "/" + entry.getKey());
                break;
            }
        }
    }

    private Path parseFilesystem(Section section) {
        Path path = null;
        for (Entry<String, String> entry : section.entrySet()) {
            if (entry.getKey().equals("path")) {
                path = Paths.get(entry.getValue());
            } else {
                barf("unknown option: " + section.getName() + "/"
                        + entry.getKey());
            }
        }
        if (path == null) {
            barf(section.getName() + ": 'path' must be set");
        }
        return path;
    }

    private static void barf(String message) {
        throw new IllegalArgumentException(message);
    }
}

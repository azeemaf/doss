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
    private List<Area> areas;
    private final Database db;

    Config(Database db, Path path) throws IOException {
        this.db = db;
        ini = new Ini(path.toFile());
        areas = new ArrayList<>();
        for (String section : ini.keySet()) {
            if (section.startsWith("area.")) {
                areas.add(parseArea(section));
            }
        }

        if (areas.isEmpty()) {
            barf("at least one storage area must be configured");
        }
    }

    public List<Area> areas() {
        return areas;
    }

    private Area parseArea(String name) throws IOException {
        Section section = ini.get(name);
        String container = "directory";
        List<Filesystem> filesystems = new ArrayList<Filesystem>();
        for (Entry<String, String> entry : section.entrySet()) {
            if (entry.getKey().equals("fs")) {
                for (String shortName : entry.getValue().split(",\\s*")) {
                    String fsName = "fs." + shortName;
                    Section fsSection = ini.get(fsName);
                    if (fsSection == null) {
                        barf("missing " + fsName + " referred to by " + name
                                + "/fs");
                    }
                    filesystems.add(parseFilesystem(fsSection));
                }
            } else if (entry.getKey().equals("container")) {
                container = entry.getValue();
            } else {
                barf("unknown option: " + name + "/" + entry.getKey());
            }
        }
        if (filesystems.isEmpty()) {
            barf(name + " needs at least one fs defined");
        }
        return new Area(db, name, filesystems, container);
    }

    private Filesystem parseFilesystem(Section section) {
        Path path = null;
        for (Entry<String, String> entry : section.entrySet()) {
            if (entry.getKey().equals("path")) {
                path = Paths.get(entry.getValue());
            } else {
                barf("unknown option: " + section.getName() + "/"
                        + entry.getKey());
            }
        }
        return new Filesystem(section.getName(), path);
    }

    private static void barf(String message) {
        throw new IllegalArgumentException(message);
    }
}

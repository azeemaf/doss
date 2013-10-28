package doss.local;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.ini4j.Ini;
import org.ini4j.Profile.Section;

public class Config {
    public static void main(String args[]) throws Exception {
        Ini ini = new Ini();
        ini.load(new StringReader(
                "[area.staging]\nfs=staging\n[fs.staging]\npath = /staging"));

        List<Area> areas = new ArrayList<>();
        for (String section : ini.keySet()) {
            if (section.startsWith("area.")) {
                areas.add(parseArea(ini, section));
            }
        }

        if (areas.isEmpty()) {
            barf("at least one storage area must be configured");
        }
    }

    private static Area parseArea(Ini ini, String name) throws IOException {
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
                    filesystems.add(parseFilesystem(ini, fsSection));
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
        return new Area(Database.open(), name, filesystems, container);
    }

    private static Filesystem parseFilesystem(Ini ini, Section section) {
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

package doss;

import doss.core.Named;

public class Client implements Named {
    private final long id;
    private final String name;
    private final String host;
    private final String access;
    private final String publicKey;

    public Client(long id, String name, String host, String access,
            String publicKey) {
        this.id = id;
        this.name = name;
        this.host = host;
        this.access = access;
        this.publicKey = publicKey;
    }

    @Override
    public long id() {
        return id;
    }

    public String name() {
        return name;
    }

    public String host() {
        return host;
    }

    public String access() {
        return access;
    }

    public String publicKey() {
        return publicKey;
    }
}

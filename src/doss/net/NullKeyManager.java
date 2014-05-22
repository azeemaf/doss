package doss.net;

import java.net.Socket;
import java.security.Principal;

import javax.net.ssl.X509ExtendedKeyManager;

/**
 * A KeyManager which throws UnsupportedOperationException.
 */
public abstract class NullKeyManager extends X509ExtendedKeyManager {

    public NullKeyManager() {
        super();
    }

    @Override
    public String chooseServerAlias(String arg0, Principal[] arg1, Socket arg2) {
        throw new UnsupportedOperationException("chooseServerAlias");
    }

    @Override
    public String[] getServerAliases(String arg0, Principal[] arg1) {
        throw new UnsupportedOperationException("getServerAliases");
    }

    @Override
    public String chooseClientAlias(String[] keyType, Principal[] issuers,
            Socket socket) {
        throw new UnsupportedOperationException("chooseClientAlias");
    }

    @Override
    public String[] getClientAliases(String keyType, Principal[] issuers) {
        throw new UnsupportedOperationException("getClientAliases");
    }
}
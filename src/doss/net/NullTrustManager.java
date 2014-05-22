package doss.net;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * A TrustManager which throws UnsupportedOperationException.
 */
abstract class NullTrustManager extends X509ExtendedTrustManager {

    public NullTrustManager() {
        super();
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType,
            SSLEngine arg2)
            throws CertificateException {
        throw new UnsupportedOperationException("checkServerTrusted2");
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType)
            throws CertificateException {
        throw new UnsupportedOperationException("checkServerTrusted3");
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType)
            throws CertificateException {
        throw new UnsupportedOperationException("checkClientTrusted");
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType,
            Socket arg2)
            throws CertificateException {
        throw new UnsupportedOperationException("checkClientTrusted2");
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType,
            SSLEngine arg2)
            throws CertificateException {
        throw new UnsupportedOperationException("checkClientTrusted3");
    }

}
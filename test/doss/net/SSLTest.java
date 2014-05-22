package doss.net;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import org.junit.Test;

public class SSLTest {

    public void server() throws Exception {
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(new KeyManager[] { new KeyMan() },
                new TrustManager[] { new TrustyMan() },
                new SecureRandom());
        ServerSocket server = sslContext.getServerSocketFactory()
                .createServerSocket(1234);

    }

    @Test
    public void test() throws Exception {
        SSLContext sslContext = SSLContext.getInstance("SSL");
        System.out.println(sslContext);
        sslContext.init(new KeyManager[] { new KeyMan() },
                new TrustManager[] { new TrustyMan() },
                new SecureRandom());
        Socket sock = sslContext.getSocketFactory().createSocket(
                "www.nla.gov.au", 443);
        Writer w = new OutputStreamWriter(sock.getOutputStream());
        w.append("GET / HTTP/1.1\r\n");
        w.append("Host: www.nla.gov.au\r\n\r\n");
        w.flush();
        BufferedReader r = new BufferedReader(new InputStreamReader(
                sock.getInputStream()));
        System.out.println(r.readLine());
        System.out.println(r.readLine());
        System.out.println(r.readLine());
        System.out.println(r.readLine());
        System.out.println(r.readLine());
        System.out.println(r.readLine());
        System.out.println(r.readLine());

        r.close();
        w.close();
        sock.close();
    }

    public static class KeyMan extends X509ExtendedKeyManager {

        @Override
        public String chooseClientAlias(String[] keyType, Principal[] issuers,
                Socket socket) {
            System.out.println("chooseClientAlias");
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String chooseServerAlias(String keyType, Principal[] issuers,
                Socket socket) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public X509Certificate[] getCertificateChain(String alias) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String[] getClientAliases(String keyType, Principal[] issuers) {
            System.out.println("getClientAliases");
            return null;
        }

        @Override
        public PrivateKey getPrivateKey(String alias) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String[] getServerAliases(String keyType, Principal[] issuers) {
            // TODO Auto-generated method stub
            return null;
        }

    }

    public static class TrustyMan extends X509ExtendedTrustManager {

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType)
                throws CertificateException {
            System.out.println("checkClientTrusted");

        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType)
                throws CertificateException {
            System.out.println("checkServerTrusted");

        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            System.out.println("getAccepted");
            return null;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain,
                String authType,
                Socket arg2) throws CertificateException {
            // TODO Auto-generated method stub

        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain,
                String authType,
                SSLEngine arg2) throws CertificateException {
            // TODO Auto-generated method stub

        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain,
                String authType,
                Socket socket) throws CertificateException {
            System.out.println("checkServerTrusted " + chain[0].getPublicKey());
            System.out.println("omg socket: " + socket);

        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain,
                String authType,
                SSLEngine arg2) throws CertificateException {
            System.out.println("checkServerTrusted2 ");

        }
    }
}

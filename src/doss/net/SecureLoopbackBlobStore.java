package doss.net;

import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import org.apache.thrift.transport.TTransportException;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import doss.BlobStore;
import doss.core.WrappedBlobStore;
import doss.local.TempBlobStore;

public class SecureLoopbackBlobStore extends WrappedBlobStore {

    private final BlobStoreServer server;

    private SecureLoopbackBlobStore(BlobStoreServer server,
            RemoteBlobStore client) {
        super(client);
        this.server = server;
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            server.close();
        }
    }

    /**
     * Creates a LoopbackBlobStore wrapping a TempBlobStore. For testing only.
     */
    public static BlobStore open() throws IOException, TTransportException {
        return open(TempBlobStore.open());
    }

    public static BlobStore open(BlobStore wrapped) throws IOException,
            TTransportException {
        SSLContext sslContext;
        try {
            sslContext = SSLContext.getInstance("SSL");
            sslContext.init(new KeyManager[] { new KeyMan() },
                    new TrustManager[] { new TrustyMan() },
                    new SecureRandom());
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        SSLServerSocket serverSocket = (SSLServerSocket) sslContext
                .getServerSocketFactory()
                .createServerSocket(1234);
        serverSocket.setNeedClientAuth(true);
        BlobStoreServer server = new BlobStoreServer(wrapped, serverSocket);
        Thread serverThread = new Thread(server);
        serverThread.start();

        Socket clientSocket = sslContext.getSocketFactory().createSocket(
                "127.0.0.1", 1234);
        RemoteBlobStore client = new RemoteBlobStore(clientSocket);
        return new SecureLoopbackBlobStore(server, client);
    }

    public static class KeyMan extends X509ExtendedKeyManager {
        final KeyPair keyPair;
        final X509Certificate cert;

        public KeyMan() {
            try {
                KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
                keyGen.initialize(2048, new SecureRandom());
                keyPair = keyGen.generateKeyPair();
                SubjectPublicKeyInfo keyInfo = SubjectPublicKeyInfo
                        .getInstance(keyPair.getPublic().getEncoded());
                X509v3CertificateBuilder certBuilder = new X509v3CertificateBuilder(
                        new X500Name("CN=issuer"), BigInteger.valueOf(System
                                .currentTimeMillis()),
                        new Date(), new Date(System.currentTimeMillis() + 30
                                * 365 * 24 * 60 * 60
                                * 1000), new X500Name("CN=snowy.nla.gov.au"),
                        keyInfo);
                ContentSigner signer = new JcaContentSignerBuilder(
                        "SHA256WithRSAEncryption").build(keyPair.getPrivate());
                cert = new JcaX509CertificateConverter()
                        .getCertificate(certBuilder.build(signer));

            } catch (NoSuchAlgorithmException | OperatorCreationException
                    | CertificateException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String chooseClientAlias(String[] keyType, Principal[] issuers,
                Socket socket) {
            System.out.println("chooseClientAlias");
            for (String kt : keyType) {
                if (kt.equals("RSA")) {
                    return "myrsa";
                }
            }
            return null;
        }

        @Override
        public String chooseServerAlias(String keyType, Principal[] issuers,
                Socket socket) {
            System.out.println("chooseServerAlias " + keyType + " " + issuers
                    + " " + socket);
            if (keyType.equals("RSA")) {
                return "myrsa";
            }
            return null;
        }

        @Override
        public X509Certificate[] getCertificateChain(String alias) {
            System.out.println("getCertificateChain");
            return new X509Certificate[] { cert };
        }

        @Override
        public String[] getClientAliases(String keyType, Principal[] issuers) {
            System.out.println("getClientAliases");
            return null;
        }

        @Override
        public PrivateKey getPrivateKey(String alias) {
            System.out.println("getPrivateKey " + alias);
            return keyPair.getPrivate();
        }

        @Override
        public String[] getServerAliases(String keyType, Principal[] issuers) {
            System.out.println("getServerAliases");
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
            return new X509Certificate[0];
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain,
                String authType,
                Socket arg2) throws CertificateException {
            System.out.println("checkClientTrusted");
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain,
                String authType,
                SSLEngine arg2) throws CertificateException {
            System.out.println("checkClientTrusted2");
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain,
                String authType,
                Socket socket) throws CertificateException {
            System.out.println("checkServerTrusted " + chain[0].getPublicKey());
            System.out.println("omg socket: " + socket);
            chain[0].checkValidity();

        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain,
                String authType,
                SSLEngine arg2) throws CertificateException {
            System.out.println("checkServerTrusted2 ");

        }
    }
}

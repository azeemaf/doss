package doss.net;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.xml.bind.DatatypeConverter;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import doss.BlobStore;
import doss.local.LocalBlobStore;
import doss.net.DossService.Iface;

public class BlobStoreServer implements Runnable, Closeable {
    final private TServer server;
    final Map<TTransport, Connection> handlers = new ConcurrentHashMap<>();
    private final ServerSocket socket;

    public BlobStoreServer(BlobStore blobStore, ServerSocket socket)
            throws IOException,
            TTransportException {
        this.socket = socket;
        TServerTransport serverTransport = new TServerSocket(socket);
        server = new TSimpleServer(
                new TServer.Args(serverTransport)
                        .processorFactory(new ProcessorFactory(blobStore)));
        server.setServerEventHandler(new TServerEventHandler() {

            @Override
            public void processContext(ServerContext arg0, TTransport arg1,
                    TTransport arg2) {
            }

            @Override
            public void preServe() {
            }

            @Override
            public void deleteContext(ServerContext context, TProtocol in,
                    TProtocol out) {
                Connection conn = (Connection) context;
                if (conn != null) {
                    handlers.remove(conn);
                    conn.disconnect();
                }
            }

            @Override
            public ServerContext createContext(TProtocol in, TProtocol out) {
                return handlers.get(in.getTransport());
            }
        });
    }

    public BlobStoreServer(BlobStore blobStore, int port, int backlog,
            InetAddress bindAddress)
            throws IOException,
            TTransportException {
        this(blobStore, openSslSocketServer(blobStore, port, backlog,
                bindAddress));
    }

    private static ServerSocket openSslSocketServer(BlobStore blobStore,
            int port, int backlog, InetAddress bindAddress) throws IOException {
        SSLContext sslContext;
        try {
            sslContext = SSLContext.getInstance("SSL");
            sslContext.init(new KeyManager[] { new KeyManager() },
                    new TrustManager[] { new TrustManager(blobStore) },
                    new SecureRandom());
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        SSLServerSocket socket = (SSLServerSocket) sslContext
                .getServerSocketFactory()
                .createServerSocket(port, backlog, bindAddress);
        socket.setNeedClientAuth(true);
        return socket;
    }

    private class ProcessorFactory extends TProcessorFactory {
        final BlobStore blobStore;

        public ProcessorFactory(BlobStore blobStore) {
            super(null);
            this.blobStore = blobStore;
        }

        @Override
        public TProcessor getProcessor(TTransport transport) {
            Connection handler = new Connection(blobStore);
            handlers.put(transport, handler);
            return new Processor(handler);
        }

        @Override
        public boolean isAsyncProcessor() {
            return false;
        }

    }

    private static class Processor extends DossService.Processor<Iface> {

        public Processor(Iface iface) {
            super(iface);
        }

        @Override
        public boolean process(TProtocol arg0, TProtocol arg1)
                throws TException {
            try {
                return super.process(arg0, arg1);
            } catch (RuntimeException e) {
                e.printStackTrace();
                throw e;
            }
        }

    }

    private static class TrustManager extends X509ExtendedTrustManager {
        BlobStore blobStore;

        public TrustManager(BlobStore blobStore) {
            this.blobStore = blobStore;
        }

        private Path getConfigPath() {
            if (blobStore instanceof LocalBlobStore) {
                return ((LocalBlobStore) blobStore).getConfigDir();
            } else {
                return RemoteBlobStore.getConfigPath();
            }
        }

        private Path getAuthorizedKeysPath() {
            return getConfigPath().resolve("authorized_keys");
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain,
                String authType, Socket socket) throws CertificateException {
            if (chain == null || chain.length == 0) {
                throw new IllegalArgumentException("empty chain");
            }
            X509Certificate cert = chain[0];
            System.out.println("checking client "
                    + cert.getSubjectX500Principal().getName());
            // check local store
            try (BufferedReader rdr = Files.newBufferedReader(getAuthorizedKeysPath(), UTF_8);) {
                String line;
                while ((line = rdr.readLine()) != null) {
                    if (!line.startsWith("doss-rsa ")) {
                        continue;
                    }
                    if (keyMatches(cert, line.split(" ")[1])) {
                        System.out.println("MATCHED!");
                        return;
                    }
                }
            } catch (NoSuchFileException | FileNotFoundException e) {
                // no key file? that's ok. we'll create it below.
            } catch (IOException | InvalidKeySpecException
                    | NoSuchAlgorithmException e) {
                e.printStackTrace();
                throw new RuntimeException("error reading authorized_keys", e);
            }
            addAuthorizedKey(cert);
            checkClientTrusted(chain, authType, socket);
        }

        private void addAuthorizedKey(X509Certificate cert) {
            try {
                Path keyFile = getAuthorizedKeysPath();
                Files.createDirectories(keyFile.getParent());
                Files.write(keyFile,
                        ("doss-rsa "
                                + DatatypeConverter.printBase64Binary(cert
                                        .getPublicKey().getEncoded()) + "\n")
                                .getBytes(),
                        StandardOpenOption.APPEND,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            } catch (IOException e1) {
                throw new RuntimeException(e1);
            }
        }

        private static boolean keyMatches(X509Certificate cert, String keyString)
                throws InvalidKeySpecException, NoSuchAlgorithmException,
                CertificateException {
            byte[] keyBytes = DatatypeConverter.parseBase64Binary(keyString);
            PublicKey key = KeyFactory.getInstance("RSA").generatePublic(
                    new X509EncodedKeySpec(keyBytes));
            try {
                cert.verify(key);
                return true;
            } catch (InvalidKeyException | NoSuchProviderException
                    | SignatureException e) {
                return false;
            }
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType)
                throws CertificateException {
            throw new UnsupportedOperationException("checkClientTrusted1");
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType)
                throws CertificateException {
            throw new UnsupportedOperationException("checkServerTrusted1");
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain,
                String authType, SSLEngine engine) throws CertificateException {
            System.out.println("checkClientTrusted2");
            throw new UnsupportedOperationException("checkClientTrusted2");
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain,
                String authType, Socket socket) throws CertificateException {
            throw new UnsupportedOperationException("checkServerTrusted3");
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain,
                String authType, SSLEngine engine) throws CertificateException {
            throw new UnsupportedOperationException("checkServerTruste4");
        }
    }

    public static class KeyManager extends NullKeyManager {
        final KeyPair keyPair;
        final X509Certificate cert;

        public KeyManager() {
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
                                * 1000), new X500Name("CN="
                                + getHostName()),
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

        private String getHostName() {
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                return "localhost";
            }
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
            return new X509Certificate[] { cert };
        }

        @Override
        public PrivateKey getPrivateKey(String alias) {
            return keyPair.getPrivate();
        }

        @Override
        public String[] getServerAliases(String keyType, Principal[] issuers) {
            String alias = chooseServerAlias(keyType, issuers, null);
            if (alias == null) {
                return null;
            } else {
                return new String[] { alias };
            }
        }

    }

    @Override
    public void run() {
        server.serve();
    }

    @Override
    public void close() {
        server.stop();
    }

    public int getPort() {
        return socket.getLocalPort();
    }

    public InetAddress getInetAddress() {
        return socket.getInetAddress();
    }
}

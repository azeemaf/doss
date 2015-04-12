package doss.net;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
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
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;
import javax.xml.bind.DatatypeConverter;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;
import doss.Client;
import doss.NoSuchBlobException;
import doss.NoSuchBlobTxException;
import doss.Writable;
import doss.core.ManagedTransaction;
import doss.core.Transaction;
import doss.core.Writables;

public class RemoteBlobStore implements BlobStore {
    private final DossService.Client client;
    private final TTransport transport;

    RemoteBlobStore(Socket socket) throws IOException,
            TTransportException {
        transport = new TSocket(socket);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new DossService.Client(protocol);
    }

    public static RemoteBlobStore open(Socket socket) throws IOException {
        try {
            return new RemoteBlobStore(socket);
        } catch (TTransportException e) {
            throw new IOException(e);
        }
    }

    static Path getConfigPath() {
        String xdgConfigHome = System.getenv("XDG_CONFIG_HOME");
        if (xdgConfigHome != null) {
            return Paths.get(xdgConfigHome, "doss");
        } else {
            return Paths.get(System.getProperty("user.home"), ".config", "doss");
        }
    }

    public static RemoteBlobStore openSecure(String host, int port)
            throws IOException {
        Path configPath = getConfigPath();
        SSLContext sslContext;
        try {
            sslContext = SSLContext.getInstance("SSL");
            sslContext.init(new KeyManager[] { new KeyManager(configPath) },
                    new TrustManager[] { new TrustManager(configPath) },
                    new SecureRandom());
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        Socket socket = sslContext.getSocketFactory().createSocket(host, port);
        return open(socket);
    }

    @Override
    public synchronized Blob get(long blobId) throws NoSuchBlobException,
            IOException {
        try {
            return new RemoteBlob(client, client.stat(blobId));
        } catch (RemoteNoSuchBlobException e) {
            throw new NoSuchBlobException(e.getBlobId());
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Blob getLegacy(Path legacyPath) throws NoSuchBlobException,
            IOException {
        try {
            return new RemoteBlob(client, client.statLegacy(legacyPath
                    .toString()));
        } catch (RemoteNoSuchBlobException e) {
            throw new NoSuchBlobException(e.getBlobId());
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BlobTx begin() {
        try {
            return new RemoteBlobTx(client.beginTx());
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BlobTx resume(long txId) throws NoSuchBlobTxException {
        // TODO: check if tx exists first?
        return new RemoteBlobTx(txId);
    }

    @Override
    public void close() {
        transport.close();
    }

    public static int transfer(ByteBuffer src, ByteBuffer dest) {
        int n = Math.min(dest.remaining(), src.remaining());
        if (n > 0) {
            ByteBuffer tmp = src.duplicate();
            tmp.limit(src.position() + n);
            dest.put(tmp);
            src.position(src.position() + n);
        }
        return n;
    }

    private class RemoteBlobTx extends ManagedTransaction implements BlobTx {
        private final long id;

        RemoteBlobTx(long id) {
            this.id = id;
        }

        @Override
        public long id() {
            return id;
        }

        @Override
        public Blob put(Writable output) throws IOException {
            try {
                final long putHandle = client.beginPut(id);
                output.writeTo(new WritableByteChannel() {

                    @Override
                    public boolean isOpen() {
                        return true;
                    }

                    @Override
                    public void close() throws IOException {
                    }

                    @Override
                    public int write(ByteBuffer b) throws IOException {
                        int nbytes = b.remaining();
                        try {
                            //
                            // FIXME: unfortunately thrift calls ByteBuffer.array() which might not be
                            // supported by b, so clone it as a workaround.
                            //
                            ByteBuffer clone = ByteBuffer.allocate(nbytes);
                            clone.put(b);
                            clone.flip();
                            client.write(putHandle, clone);
                            b.position(b.limit());
                        } catch (TException e) {
                            throw new RuntimeException(e);
                        }
                        return nbytes;
                    }
                });
                return get(client.finishPut(putHandle));
            } catch (TException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Blob put(Path source) throws IOException {
            return put(Writables.wrap(source));
        }

        @Override
        public Blob put(byte[] bytes) throws IOException {
            return put(Writables.wrap(bytes));
        }

        @Override
        protected Transaction getCallbacks() {
            return new Transaction() {

                @Override
                public void rollback() throws IOException {
                    try {
                        client.rollbackTx(id);
                    } catch (TException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void prepare() throws IOException {
                    try {
                        client.prepareTx(id);
                    } catch (TException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void commit() throws IOException {
                    try {
                        client.commitTx(id);
                    } catch (TException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void close() throws IOException {
                }
            };
        }

    }

    @Override
    public Iterable<Client> clients() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("TODO");
    }

    public static class KeyManager extends NullKeyManager {
        final KeyPair keyPair;
        final X509Certificate cert;

        public KeyManager(Path savePath) {
            X509Certificate cert;
            KeyPair keyPair;
            try {
                cert = loadCert(savePath);
                PrivateKey key = loadKey(savePath);
                keyPair = new KeyPair(cert.getPublicKey(), key);
            } catch (NoSuchFileException e) {
                System.err.println("DOSS: generating new client key...");
                keyPair = generateKeyPair();
                cert = selfSign(keyPair);
                saveKeyAndCert(savePath, keyPair.getPrivate(), cert);
            }
            this.cert = cert;
            this.keyPair = keyPair;
        }

        private static X509Certificate loadCert(Path savePath)
                throws NoSuchFileException {
            Path certPath = savePath.resolve("client.crt");
            try (InputStream in = Files.newInputStream(certPath)) {
                return (X509Certificate) CertificateFactory
                        .getInstance("X.509")
                        .generateCertificate(in);
            } catch (NoSuchFileException e) {
                throw e;
            } catch (IOException | CertificateException e) {
                throw new RuntimeException("failed to load DOSS client cert: "
                        + certPath, e);
            }
        }

        private static PrivateKey loadKey(Path savePath)
                throws NoSuchFileException {
            Path keyPath = savePath.resolve("client.key");
            try {
                KeyFactory rsaFactory = KeyFactory.getInstance("RSA");
                byte[] encodedKey = Files.readAllBytes(keyPath);
                return rsaFactory.generatePrivate(new PKCS8EncodedKeySpec(
                        encodedKey));
            } catch (NoSuchFileException e) {
                throw e;
            } catch (InvalidKeySpecException | NoSuchAlgorithmException
                    | IOException e) {
                throw new RuntimeException("failed to load DOSS client key: "
                        + keyPath, e);
            }
        }

        private static final Set<StandardOpenOption> options = new HashSet<>(Arrays.asList(
                CREATE, TRUNCATE_EXISTING, WRITE));
        private static final Set<PosixFilePermission> perms = PosixFilePermissions
                .fromString("rw-------");

        private static void saveKeyAndCert(Path savePath, PrivateKey key,
                X509Certificate cert) {
            try {
                if (!Files.exists(savePath)) {
                    Files.createDirectories(savePath);
                }
                try (ByteChannel chan = Files.newByteChannel(savePath.resolve("client.key"),
                        options, PosixFilePermissions.asFileAttribute(perms))) {
                    chan.write(ByteBuffer.wrap(key.getEncoded()));
                }
                Files.write(savePath.resolve("client.crt"), cert.getEncoded(), CREATE,
                        TRUNCATE_EXISTING, WRITE);
            } catch (IOException | CertificateEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        private static KeyPair generateKeyPair() {
            try {
                KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
                keyGen.initialize(2048, new SecureRandom());
                return keyGen.generateKeyPair();
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        private static String getHostName() {
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                throw new RuntimeException(
                        "Unable to get local hostname to generate SSL certificate", e);
            }
        }

        private static X509Certificate selfSign(KeyPair keyPair) {
            SubjectPublicKeyInfo keyInfo = SubjectPublicKeyInfo
                    .getInstance(keyPair.getPublic().getEncoded());
            X509v3CertificateBuilder certBuilder = new X509v3CertificateBuilder(
                    new X500Name("CN=issuer"), BigInteger.valueOf(System
                            .currentTimeMillis()),
                    new Date(), new Date(System.currentTimeMillis() + 30
                            * 365 * 24 * 60 * 60
                            * 1000), new X500Name("CN=" + getHostName()),
                    keyInfo);
            ContentSigner signer;
            try {
                signer = new JcaContentSignerBuilder(
                        "SHA256WithRSAEncryption").build(keyPair.getPrivate());
                return new JcaX509CertificateConverter()
                        .getCertificate(certBuilder.build(signer));
            } catch (OperatorCreationException | CertificateException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String chooseClientAlias(String[] keyType, Principal[] arg1,
                Socket arg2) {
            System.out.println("choose client");
            for (String kt : keyType) {
                if (kt.equals("RSA")) {
                    return "myrsa";
                }
            }
            return null;
        }

        @Override
        public X509Certificate[] getCertificateChain(String arg0) {
            return new X509Certificate[] { cert };
        }

        @Override
        public PrivateKey getPrivateKey(String arg0) {
            return keyPair.getPrivate();
        }
    }

    private static class KnownHost {
        final private static Pattern PATTERN = Pattern
                .compile("^(?<host>[^, ]+),(?<ip>[^, ]+) doss-rsa (?<key>[^ ]+)$");

        public final String host, ip;
        public final PublicKey publicKey;
        public final int lineno;

        public KnownHost(String line, int lineno) {
            Matcher matcher = PATTERN.matcher(line.trim());
            if (matcher.matches()) {
                host = matcher.group("host");
                ip = matcher.group("ip");
                publicKey = decodeKey(matcher.group("key"));
                this.lineno = lineno;
            } else {
                throw new IllegalArgumentException("malformed known_hosts line " + lineno);
            }
        }

        public KnownHost(String host, String ip, PublicKey publicKey) {
            this.host = host;
            this.ip = ip;
            this.publicKey = publicKey;
            this.lineno = 0;
        }

        private static PublicKey decodeKey(String base64Key) {
            byte[] key = DatatypeConverter.parseBase64Binary(base64Key);
            try {
                return Crypto.RSA.generatePublic(new X509EncodedKeySpec(key));
            } catch (InvalidKeySpecException e) {
                throw new RuntimeException(e);
            }
        }

        public boolean matches(String hostToFind, String ipToFind) {
            return (host != null && host.equals(hostToFind)) || ipToFind.equals(ip);
        }

        @Override
        public String toString() {
            if (host != null && host.indexOf(" ") >= 0) {
                throw new IllegalArgumentException("bad host: " + host);
            }
            if (ip.indexOf(" ") >= 0) {
                throw new IllegalArgumentException("bad ip: " + ip);
            }
            String key = DatatypeConverter.printBase64Binary(publicKey.getEncoded());
            if (host != null) {
                return host + "," + ip + " doss-rsa " + key;
            } else {
                return ip + " doss-rsa " + key;
            }
        }
    }

    public static class TrustManager extends NullTrustManager {
        final private static Charset UTF8 = Charset.forName("utf-8");

        final Path knownHostsPath;

        public TrustManager(Path configPath) {
            this.knownHostsPath = configPath.resolve("known_hosts");
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
                throws CertificateException {
            chain[0].checkValidity();

            InetSocketAddress addr = (InetSocketAddress) socket.getRemoteSocketAddress();
            KnownHost knownHost;
            try {
                knownHost = lookupKnownHosts(addr);
            } catch (NoSuchFileException e) {
                knownHost = null;
            } catch (IOException e) {
                throw new CertificateException("error reading " + knownHostsPath, e);
            }
            if (knownHost == null) {
                knownHost = new KnownHost(addr.getHostString(), addr.getAddress().getHostAddress(),
                        chain[0].getPublicKey());
                try {
                    Files.write(knownHostsPath, knownHost.toString().getBytes(UTF8),
                            StandardOpenOption.APPEND,
                            StandardOpenOption.CREATE);
                } catch (IOException e) {
                    throw new CertificateException("failed adding known hosts entry", e);
                }
                System.err.println("DOSS: added " + knownHost.host + " (" + knownHost.ip + ") to "
                        + knownHostsPath);
                System.err.println("DOSS: server key fingerprint is "
                        + Crypto.fingerprint(knownHost.publicKey));
            } else {
                try {
                    chain[0].verify(knownHost.publicKey);
                } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchProviderException
                        | SignatureException e) {
                    throw new CertificateException("error verifying server's certificate against "
                            + knownHostsPath
                            + ":" + knownHost.lineno, e);
                }
            }
        }

        private KnownHost lookupKnownHosts(SocketAddress socketAddress) throws IOException {
            InetSocketAddress address = (InetSocketAddress) socketAddress;
            String hostToFind = address.getHostString();
            String ipToFind = address.getAddress().getHostAddress();
            try (BufferedReader rdr = Files.newBufferedReader(knownHostsPath, UTF8)) {
                int lineno = 1;
                for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                    KnownHost entry = new KnownHost(line, lineno++);
                    if (entry.matches(hostToFind, ipToFind)) {
                        return entry;
                    }
                }
            }
            return null;
        }
    }
}

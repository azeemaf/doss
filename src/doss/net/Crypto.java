package doss.net;

import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;

import javax.xml.bind.DatatypeConverter;

public class Crypto {
    final public static KeyFactory RSA;

    static {
        try {
            RSA = KeyFactory.getInstance("RSA");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static String fingerprint(PublicKey key) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA1");
            return DatatypeConverter.printHexBinary(digest.digest(key.getEncoded()));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}

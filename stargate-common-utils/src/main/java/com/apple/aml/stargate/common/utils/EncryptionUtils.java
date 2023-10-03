package com.apple.aml.stargate.common.utils;

import com.google.common.base.Splitter;
import org.apache.commons.lang3.tuple.Pair;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.bouncycastle.util.io.pem.PemWriter;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.util.Arrays;
import java.util.Base64;

import static com.apple.aml.stargate.common.constants.CommonConstants.RsaHeaders.BEGIN_PRIVATE_KEY;
import static com.apple.aml.stargate.common.constants.CommonConstants.RsaHeaders.BEGIN_PUBLIC_KEY;
import static com.apple.aml.stargate.common.constants.CommonConstants.RsaHeaders.END_PRIVATE_KEY;
import static com.apple.aml.stargate.common.constants.CommonConstants.RsaHeaders.END_PUBLIC_KEY;
import static com.apple.aml.stargate.common.constants.CommonConstants.RsaHeaders.PRIVATE_KEY_HEADER_INCLUDES;
import static com.apple.aml.stargate.common.constants.CommonConstants.RsaHeaders.PUBLIC_KEY_HEADER_INCLUDES;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class EncryptionUtils {
    private static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 16;

    private EncryptionUtils() {

    }

    public static String toPEMPublicKey(final String publicKey) {
        if (publicKey.contains(PUBLIC_KEY_HEADER_INCLUDES)) {
            return publicKey;
        }
        StringBuilder publicKeyFormatted = new StringBuilder(BEGIN_PUBLIC_KEY + "\n");
        for (final String row : Splitter.fixedLength(64).split(publicKey)) {
            publicKeyFormatted.append(row).append("\n");
        }
        publicKeyFormatted.append(END_PUBLIC_KEY + "\n");
        return publicKeyFormatted.toString();
    }

    public static RSAPublicKey getPublicKeyFromEncodedPemPrivateKey(final String encodedPEMString) throws InvalidKeySpecException, NoSuchAlgorithmException {
        byte[] decodePEMPrivateKey = Base64.getDecoder().decode(encodedPEMString);
        String pemPrivateKey = new String(decodePEMPrivateKey, UTF_8);
        RSAPrivateKey privateKey = getPrivateKeyFromPEMString(pemPrivateKey);
        return getPublicKeyFromPrivateKey(privateKey);
    }

    public static RSAPrivateKey getPrivateKeyFromPEMString(final String pemKey) throws NoSuchAlgorithmException, InvalidKeySpecException {
        String privateKeyS = pemKey.replace(BEGIN_PRIVATE_KEY, "").replaceAll(System.lineSeparator(), "").replace(END_PRIVATE_KEY, "");
        byte[] encoded = Base64.getDecoder().decode(privateKeyS);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
        return (RSAPrivateKey) keyFactory.generatePrivate(keySpec);
    }

    public static RSAPublicKey getPublicKeyFromPrivateKey(final RSAPrivateKey privateKey) throws NoSuchAlgorithmException, InvalidKeySpecException {
        RSAPrivateCrtKey privateCrtKey = (RSAPrivateCrtKey) privateKey;
        RSAPublicKeySpec publicKeySpec = new java.security.spec.RSAPublicKeySpec(privateCrtKey.getModulus(), privateCrtKey.getPublicExponent());
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return (RSAPublicKey) keyFactory.generatePublic(publicKeySpec);
    }

    public static Pair<String, String> encodedRSAKeyPair() throws Exception {
        KeyPair keyPair = generateRSAKeyPairs(2048);
        Key publicKey = keyPair.getPublic();
        Key privateKey = keyPair.getPrivate();
        Base64.Encoder encoder = Base64.getEncoder();
        String encodedPublicKey = encoder.encodeToString(publicKey.getEncoded());
        String singleEncodedPrivateKey = encoder.encodeToString(privateKey.getEncoded());
        String encodedPrivateKey = encoder.encodeToString(toPEMPrivateKey(singleEncodedPrivateKey).getBytes(UTF_8));
        return Pair.of(encodedPublicKey, encodedPrivateKey);
    }

    public static KeyPair generateRSAKeyPairs(final int keySize) throws NoSuchAlgorithmException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(keySize);
        return kpg.generateKeyPair();
    }

    public static String toPEMPrivateKey(final String privateKey) {
        if (privateKey.contains(PRIVATE_KEY_HEADER_INCLUDES)) {
            return privateKey;
        }
        StringBuilder privateKeyFormatted = new StringBuilder(BEGIN_PRIVATE_KEY + "\n");
        for (final String row : Splitter.fixedLength(64).split(privateKey)) {
            privateKeyFormatted.append(row).append("\n");
        }
        privateKeyFormatted.append(END_PRIVATE_KEY + "\n");
        return privateKeyFormatted.toString();
    }

    public static String pemString(final Key key, final String description) throws IOException {
        StringWriter writer = new StringWriter();
        PemWriter pemWriter = new PemWriter(writer);
        pemWriter.writeObject(new PemObject(description, key.getEncoded()));
        pemWriter.close();
        return writer.toString();
    }

    public static RSAPrivateKey privateKey(final String pemString) throws Exception {
        StringReader reader = new StringReader(pemString);
        PemReader pemReader = new PemReader(reader);
        byte[] content = pemReader.readPemObject().getContent();
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(content);
        return (RSAPrivateKey) keyFactory.generatePrivate(keySpec);
    }

    public static RSAPublicKey publicKey(final String pemString) throws Exception {
        StringReader reader = new StringReader(pemString);
        PemReader pemReader = new PemReader(reader);
        byte[] content = pemReader.readPemObject().getContent();
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(content);
        return (RSAPublicKey) keyFactory.generatePublic(keySpec);
    }

    public static SecretKeySpec aesNullableKeySpec(final String key) {
        try {
            return aesKeySpec(key);
        } catch (Exception e) {
            return null;
        }
    }

    public static SecretKeySpec aesKeySpec(final String key) throws Exception {
        byte[] bytes = key.getBytes(UTF_8);
        MessageDigest sha = MessageDigest.getInstance("SHA-1");
        bytes = sha.digest(bytes);
        bytes = Arrays.copyOf(bytes, 16);
        return new SecretKeySpec(bytes, "AES");
    }

    public static Pair<String, String> defaultEncrypt(final String input, final SecretKeySpec spec) throws Exception {
        Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION);

        byte[] initVec = initializationVector();
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, initVec);

        cipher.init(Cipher.ENCRYPT_MODE, spec, gcmParameterSpec);
        return Pair.of(Base64.getEncoder().encodeToString(cipher.doFinal(input.getBytes(UTF_8))),
                       Base64.getEncoder().encodeToString(initVec));
    }

    public static String defaultDecrypt(final String input, final SecretKeySpec spec, final String encodedInitVec) throws Exception {
        Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
        byte[] initVec = Base64.getDecoder().decode(encodedInitVec);

        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, initVec);

        cipher.init(Cipher.DECRYPT_MODE, spec, gcmParameterSpec);
        return new String(cipher.doFinal(Base64.getDecoder().decode(input)), UTF_8);
    }

    private static byte[] initializationVector() {
        byte[] initVector = new byte[GCM_IV_LENGTH];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(initVector);

        return initVector;
    }
}

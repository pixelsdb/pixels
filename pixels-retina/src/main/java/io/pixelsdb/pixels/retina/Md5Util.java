package io.pixelsdb.pixels.retina;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Md5Util
{
    private Md5Util() {}

    public static String md5(byte[] data)
    {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(data);
            return bytesToHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }

    public static String md5(ByteBuffer buffer)
    {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");

            ByteBuffer copy = buffer.asReadOnlyBuffer();

            if (copy.hasArray()) {
                md.update(copy.array(), copy.position(), copy.remaining());
            } else {
                byte[] tmp = new byte[8192];
                while (copy.hasRemaining()) {
                    int len = Math.min(copy.remaining(), tmp.length);
                    copy.get(tmp, 0, len);
                    md.update(tmp, 0, len);
                }
            }

            return bytesToHex(md.digest());
        } catch (Exception e) {
            throw new RuntimeException("Failed to compute MD5 from ByteBuffer", e);
        }
    }

    private static String bytesToHex(byte[] bytes)
    {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}

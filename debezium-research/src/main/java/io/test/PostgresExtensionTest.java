package io.test;


import java.io.IOException;
import java.io.InputStream;

public class PostgresExtensionTest {

    public static void main(String[] args) throws Exception {
        // 读取create table的字节流
        byte[] totalBytes = readAllBytes();

        // todo 解析
    }

    private static byte[] readAllBytes() throws IOException {
        byte[] totalBytes = new byte[0];
        try (InputStream is = PostgresExtensionTest.class.getClassLoader().getResourceAsStream("pglogical2.bin")) {
            byte[] cache = new byte[1024];
            int len = 0;
            while ((len = is.read(cache)) != -1) {
                int available = totalBytes.length;
                byte[] newTotalBytes = totalBytes.length == 0 ? new byte[len] : new byte[totalBytes.length + len];
                if (available != 0) {
                    System.arraycopy(totalBytes, 0, newTotalBytes, 0, totalBytes.length);
                }
                totalBytes = newTotalBytes;
                System.arraycopy(cache, 0, totalBytes, available, len);
            }
        }
        return totalBytes;
    }
}

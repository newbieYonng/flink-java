package com.yonng.test;

import java.io.IOException;
import java.io.RandomAccessFile;

public class RandomAccessFileDemo {

    public static void main(String[] args) throws IOException {

        /*RandomAccessFile raf1 = new RandomAccessFile("data/a.png", "r");
        RandomAccessFile raf2 = new RandomAccessFile("data/a_copy.png", "rw");

        byte[] bytes = new byte[1024];
        int len;
        while ((len = raf1.read(bytes)) != -1) {
            raf2.write(bytes, 0, len);
        }
        raf2.close();
        raf1.close();*/

        RandomAccessFile raf1 = new RandomAccessFile("data/a.txt", "rw");
        raf1.seek(3);
        StringBuilder builder = new StringBuilder();
        byte[] bytes = new byte[20];
        int len;
        while ((len = raf1.read(bytes)) != -1)
            builder.append(new String(bytes, 0, len));
        raf1.seek(3);
        raf1.write("aaa".getBytes());
        raf1.write(builder.toString().getBytes());
        raf1.close();
    }
}

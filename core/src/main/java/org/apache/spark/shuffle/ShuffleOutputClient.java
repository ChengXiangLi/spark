package org.apache.spark.shuffle;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

public class ShuffleOutputClient {

    SocketChannel sc = null;

    public ShuffleOutputClient(String host, int port) {
        SocketAddress sad = new InetSocketAddress(host, port);
        try {
            sc = SocketChannel.open();
            sc.connect(sad);
            sc.configureBlocking(true);
            sc.finishConnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void Sendfile(String filePath, String targetPath) throws IOException {

        int targetPathSize = targetPath.getBytes().length;
        ByteBuffer bb = ByteBuffer.allocate(targetPathSize + 4);
        bb.putInt(targetPathSize);
        bb.put(targetPath.getBytes());
        bb.flip();
        sc.write(bb);

        long fsize = 100000L, sendzise = 4094;

        FileChannel fc = new FileInputStream(filePath).getChannel();
        long start = System.currentTimeMillis();
        long nsent = 0, curnset = 0;
        curnset = fc.transferTo(0, fsize, sc);
        System.out.println("total bytes transferred--" + curnset + " and time taken in MS--" + (System.currentTimeMillis() - start));
        fc.close();
    }
}

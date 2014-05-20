package org.apache.spark.shuffle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

public class ShuffleOutputClient {
    Logger log = LoggerFactory.getLogger(this.getClass());
    SocketChannel sc = null;
    String host;
    int port;

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public ShuffleOutputClient(String host, int port) {
        this.host = host;
        this.port = port;
        SocketAddress sad = new InetSocketAddress(host, port);
        try {
            sc = SocketChannel.open();
            sc.connect(sad);
            sc.configureBlocking(true);
            sc.finishConnect();
            System.out.printf("connect to shuffle output server %s:%d\n", host, port);
        } catch (IOException e) {
            System.out.printf("failed connect to shuffle output server %s:%d\n", host, port);
            e.printStackTrace();
        }
    }

    public void sendFile(String filePath, String targetPath) throws IOException {

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
        System.out.printf("finished transfer data from:" + filePath + ", to node:" + host + ", remote path:" + targetPath +"\n");
        System.out.println("total bytes transferred--" + curnset + " and time taken in MS--" + (System.currentTimeMillis() - start));
        fc.close();
        sc.close();
    }
}

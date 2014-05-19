package org.apache.spark.shuffle;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;


public class ShuffleOutpuServer {
    ServerSocketChannel listener = null;

    public void mySetup() {
        InetSocketAddress listenAddr = new InetSocketAddress(9026);

        try {
            listener = ServerSocketChannel.open();
            ServerSocket ss = listener.socket();
            ss.setReuseAddress(true);
            ss.bind(listenAddr);
            System.out.println("Listening on port : " + listenAddr.toString());
        } catch (IOException e) {
            System.out.println("Failed to bind, is port : " + listenAddr.toString()
                    + " already in use ? Error Msg : " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ShuffleOutpuServer dns = new ShuffleOutpuServer();
        dns.mySetup();
        dns.readData();
    }

    public void readData() {
        ByteBuffer dst = ByteBuffer.allocate(4096);
        try {
            while (true) {
                SocketChannel conn = listener.accept();
                System.out.println("Accepted : " + conn);
                conn.configureBlocking(true);

                int nread = conn.read(dst);
                dst.rewind();
                int size = dst.getInt();
                byte[] pathByte = new byte[size];
                dst.get(pathByte);
                String path = new String(pathByte);

                File file = new File(path);
                if(file.exists()) {
                    file.delete();
                }
                FileOutputStream outputStream = new FileOutputStream(file, false);
                int len = nread - 4 - size;
                if(len >0) {
                    outputStream.write(dst.array(), 4 + size, len);
                }
                nread = 0;
                while (nread != -1) {
                    dst.rewind();
                    byte[] array = dst.array();
                    System.out.printf("array size[%d], nread[%d]\n", array.length, nread);
                    outputStream.write(array, 0, nread);
                    dst.clear();
                    try {
                        nread = conn.read(dst);
                    } catch (IOException e) {
                        e.printStackTrace();
                        nread = -1;
                    }
                }

                outputStream.flush();
                outputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

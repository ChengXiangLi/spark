package org.apache.spark.shuffle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class ShuffleOutputServer {
    Logger log = LoggerFactory.getLogger(this.getClass());
    ServerSocketChannel listener = null;
    Executor executor = Executors.newFixedThreadPool(4);
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    Map<String, Queue<String>> allAvailableFiles = new HashMap<String, Queue<String>>();
    private int MAX_REDUCE_FILE_NUMBER_PER_PARTITION = 4;

    public void mySetup() {
        InetSocketAddress listenAddr = new InetSocketAddress(9026);

        try {
            listener = ServerSocketChannel.open();
            ServerSocket ss = listener.socket();
            ss.setReuseAddress(true);
            ss.bind(listenAddr);
            log.info("Listening on port : " + listenAddr.toString());
        } catch (IOException e) {
            log.error("Failed to bind, is port : " + listenAddr.toString()
                    + " already in use ? Error Msg : " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ShuffleOutputServer dns = new ShuffleOutputServer();
        dns.mySetup();
        dns.readData();
    }

    public void readData() {

        try {
            while (true) {
                SocketChannel conn = listener.accept();
                System.out.println("Accepted : " + conn);
                conn.configureBlocking(true);
                executor.execute(new OutputPersistentRunnable(conn));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    class OutputPersistentRunnable implements Runnable {

        private SocketChannel conn;

        public OutputPersistentRunnable(SocketChannel conn) {
            this.conn = conn;
        }

        @Override
        public void run() {
            try {
                readOutput(conn);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void readOutput(SocketChannel conn) throws IOException {
        ByteBuffer dst = ByteBuffer.allocate(4096000);
        int nread = conn.read(dst);
        dst.rewind();
        int size = dst.getInt();
        byte[] pathByte = new byte[size];
        dst.get(pathByte);
        String path = new String(pathByte);
        System.out.println("start to write file:" + path);
        File file = new File(path);
        String fileParent = file.getParent();
        File parent = new File(fileParent);
        if (!parent.exists()) {
            parent.mkdirs();
        }
        try {
            lock.writeLock().lock();
            Queue<String> availableFiles = allAvailableFiles.get(fileParent);
            if (availableFiles == null) {
                availableFiles = new LinkedList<String>();
                for (int i=0; i<MAX_REDUCE_FILE_NUMBER_PER_PARTITION; i++) {
                    availableFiles.add(Integer.toString(i));
                }
                allAvailableFiles.put(fileParent, availableFiles);
            }
            String fileName = availableFiles.poll();
            file = new File(parent, fileName);
        } finally {
            lock.writeLock().unlock();
        }
        if(file.exists()) {
            file.createNewFile();
        }
        FileOutputStream outputStream = new FileOutputStream(file, false);
        int len = nread - 4 - size;
        if (len > 0) {
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
        System.out.println("finished write file:" + path);
        try {
            lock.writeLock().lock();
            Queue<String> availableFiles = allAvailableFiles.get(fileParent);
            availableFiles.add(file.getName());
        } finally {
            lock.writeLock().unlock();
        }
    }
}

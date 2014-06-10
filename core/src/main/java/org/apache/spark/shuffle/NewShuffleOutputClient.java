package org.apache.spark.shuffle;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

public class NewShuffleOutputClient {
    Logger log = LoggerFactory.getLogger(this.getClass());
    SocketChannel sc = null;
    String host, blockId, sourceFile;
    int port;

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public NewShuffleOutputClient(String host, int port, String sourceFile, String blockId) {
        this.host = host;
        this.port = port;
        this.blockId = blockId;
        this.sourceFile = sourceFile;
    }

    public void run() {
        SocketAddress sad = new InetSocketAddress(host, port);
        try {
            sc = SocketChannel.open();
            sc.configureBlocking(false);
            sc.connect(sad);
            System.out.printf("connect to shuffle output server %s:%d\n", host, port);
            Selector selector = Selector.open();
            sc.register(selector, SelectionKey.OP_CONNECT);
            ByteBuffer buffer = ByteBuffer.allocate(50);
            StringBuilder sb = new StringBuilder();
            boolean finished = false;
            while (true) {
                int num = selector.select();
                if (num <= 0) {
                    continue;
                }
                Iterator iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = (SelectionKey) iter.next();
                    iter.remove();
                    if (key.isConnectable()) {
                        SocketChannel channel = (SocketChannel) key
                                .channel();
                        if (channel.isConnectionPending()) {
                            channel.finishConnect();
                        }
                        sendFile(channel, blockId);

                        channel.register(selector, SelectionKey.OP_READ);
                    } else if (key.isReadable()) {
                        SocketChannel channel = (SocketChannel) key
                                .channel();
                        buffer.clear();
                        int count = channel.read(buffer);
                        buffer.flip();
                        while (buffer.hasRemaining()) {
                            sb.append((char) buffer.get());
                        }
                            sc.close();
                            finished = true;
                            break;
                    }
                }

                if (finished) {
                    break;
                }
            }
            System.out.printf("finished send file:%s\n", sb.toString());
        } catch (IOException e) {
            System.out.printf("failed connect to shuffle output server %s:%d\n", host, port);
            e.printStackTrace();
        }
    }

    private void sendFile(SocketChannel channel, String blockId) throws IOException {
        long start = System.currentTimeMillis();
        File file = new File(this.sourceFile);
        long size = file.length();

        ByteBuffer headerBuffer = ByteBuffer.allocate(50);
        headerBuffer.putInt((int) size);
        headerBuffer.putInt(blockId.length());
        headerBuffer.put(blockId.getBytes());
//        int position = headerBuffer.position();
//        for (; position < 50; position++) {
//            headerBuffer.put((byte) 0);
//        }

        headerBuffer.flip();
        channel.write(headerBuffer);

        FileChannel fc = new FileInputStream(file).getChannel();
        ByteBuffer fileBuffer = ByteBuffer.allocate(4096000);
        int count = fc.read(fileBuffer);
        int writedSize = 0;
        while (count > 0) {
            fileBuffer.flip();
            while (fileBuffer.hasRemaining()) {
                writedSize += channel.write(fileBuffer);
            }
            fileBuffer.clear();
            count = fc.read(fileBuffer);
        }
//        long transferedSize = fc.transferTo(0, size, channel);
        fc.close();
        long cost = System.currentTimeMillis() - start;
        String threadName = Thread.currentThread().getName();

        System.out.printf("transfer to host:%s, port:%d, thread:%s, file:%s, size:%s, blockId:%s, cost:%dms.\n",
                host, port, threadName, this.sourceFile, writedSize, blockId, cost);
//        this.sc.close();
    }

    public static void main(String[] args) {
        final String serverhost = args[0];
        final String file = args[1];
        long start = System.currentTimeMillis();
        int threadNumber = Integer.parseInt(args[2]);
        ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder().setDaemon(false);
        ThreadFactory threadFactory = threadFactoryBuilder.setNameFormat("thread-%d").build();
        ExecutorService executorService = Executors.newFixedThreadPool(threadNumber, threadFactory);
        List<Future> futures = Lists.newArrayList();
        for (int i = 0; i < threadNumber; i++) {
            final int index = i;
            Future f = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    new NewShuffleOutputClient(serverhost, 9026, file, "shuffle0-" + index+"-0").run();
                }
            });
            futures.add(f);
        }
        executorService.shutdown();
        for (Future f : futures) {
            try {
                f.get();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (ExecutionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        long cost = System.currentTimeMillis() - start;
        System.out.println("transfer " + threadNumber + " files totally cost:" + cost + "ms.");
    }
}

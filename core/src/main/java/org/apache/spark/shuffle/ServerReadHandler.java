package org.apache.spark.shuffle;

import org.apache.spark.SparkEnv;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by chengxia on 2014/6/6.
 */
class ServerReadHandler {
    private String rootDir = "/home/spark/local";
    private boolean alreadyReadHead = false;
    private String blockId;
    private int fileSize;
    private String fileName;
    private FileChannel fileChannel;
    private int totalWrite = 0;

    public ServerReadHandler(String fileName) {
        this.fileName = fileName;
        this.rootDir = "/home/spark/local";
    }

    public void handleEvent(ServerDataEvent event) {
        try {
            if (read(event.data)) {
                // Return to sender
                event.server.send(event.socket, blockId.getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean read(byte[] data) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        if (alreadyReadHead) {
            return readBody(buffer);
        } else {
            readHead(buffer);
            return readBody(buffer);
        }
    }

    private boolean readBody(ByteBuffer buffer) throws IOException {
        totalWrite += fileChannel.write(buffer);
        if (totalWrite == fileSize) {
            System.out.printf("finished store received data for blockId:%s\n", blockId);
            closeFileChannel();
            return true;
        }
        return false;
    }

    private boolean readHead(ByteBuffer buffer) throws IOException {
        fileSize = buffer.getInt();
        int idLength = buffer.getInt();
        StringBuilder sb = new StringBuilder(idLength);
        for (int i = 0; i < idLength; i++) {
            sb.append((char) buffer.get());
        }
        blockId = sb.toString();
        alreadyReadHead = true;
        initFileChannel(blockId);
        System.out.printf("finished parse header buffer, file length:%d, blockId:%s\n", fileSize, blockId);
        return true;
    }

    private void initFileChannel(String blockId) throws FileNotFoundException {
        String pathSeparator = System.getProperties().getProperty("path.deparator", "/");
        String parent = rootDir + pathSeparator + getShuffleIdByShuffleFilename(blockId) +
                pathSeparator + getReduceIdByShuffleFileName(blockId);

        File file = new File(parent, fileName);
        if (file.exists()) {
            file.delete();
        } else if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        FileOutputStream fos = new FileOutputStream(file, true);
        fileChannel = fos.getChannel();
    }

    private void closeFileChannel() {
        try {
            fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getReduceIdByShuffleFileName(String fileName) {
        return fileName.split("_")[3];
    }

    private String getShuffleIdByShuffleFilename(String fileName) {
        return fileName.split("_")[1];
    }
}

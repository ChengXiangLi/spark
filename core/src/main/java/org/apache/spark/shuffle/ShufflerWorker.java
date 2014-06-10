package org.apache.spark.shuffle;

import java.nio.channels.SocketChannel;
import java.util.*;

public class ShufflerWorker implements Runnable {
	private List queue = new LinkedList();
    private Map<SocketChannel, ServerReadHandler> channelToHandlers = new HashMap<SocketChannel, ServerReadHandler>();
    private String fileName;

    public ShufflerWorker(String fileName) {
        this.fileName = fileName;
    }

	public void processData(NewShuffleOutputServer server, SocketChannel socket, byte[] data, int count) {
		byte[] dataCopy = new byte[count];
		System.arraycopy(data, 0, dataCopy, 0, count);
		synchronized(queue) {
			queue.add(new ServerDataEvent(server, socket, dataCopy));
			queue.notify();
		}
	}
	
	public void run() {
        System.out.printf("started shuffler worker with reduce file:%s.\n", fileName);
		ServerDataEvent dataEvent;
		
		while(true) {
			// Wait for data to become available
			synchronized(queue) {
				while(queue.isEmpty()) {
					try {
						queue.wait();
					} catch (InterruptedException e) {
					}
				}
				dataEvent = (ServerDataEvent) queue.remove(0);
			}
			
			ServerReadHandler handler = channelToHandlers.get(dataEvent.socket);
            if (handler == null) {
                handler = new ServerReadHandler(fileName);
                channelToHandlers.put(dataEvent.socket, handler);
            }
            handler.handleEvent(dataEvent);
		}
	}
}

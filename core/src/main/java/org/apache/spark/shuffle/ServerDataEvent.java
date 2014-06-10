package org.apache.spark.shuffle;

import java.nio.channels.SocketChannel;

class ServerDataEvent {
	public NewShuffleOutputServer server;
	public SocketChannel socket;
	public byte[] data;
	
	public ServerDataEvent(NewShuffleOutputServer server, SocketChannel socket, byte[] data) {
		this.server = server;
		this.socket = socket;
		this.data = data;
	}
}
package csx55.overlay.transport;

import java.net.Socket;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import csx55.overlay.util.Packet;
import csx55.overlay.node.Node;

public class TCPSenderThread implements Runnable{
    private boolean isRunning;
    private final BlockingQueue<Packet> queue;
    private final Node node;

    public TCPSenderThread(Node node) throws IOException{
        this.isRunning = true;
        this.node = node;
        this.queue = new LinkedBlockingQueue<>();
    }

    // Atomicly adds a new item into the queue to be sent
    public void addToQueue(Packet packet) {
        try{
            queue.put(packet);
        } catch (InterruptedException ie) {
            System.err.println("TCPSenderThread.java: " + ie.getMessage());
        }
    }

    public void stop() {
        this.isRunning = false;
    }

    public int getQueueSize() {
        return queue.size();
    }

    @Override
    public void run() {
        while (isRunning) {
            try{
                // Take a packet off the queue containing (destination, byte[])
                // Get the appropriate TCPSender and send data
                Packet p = queue.take();
                
                String key = p.getKey();
                byte[] message = p.getMessage();
                
                TCPSender sender = node.getConnectedNodes().get(key).getSender();
                sender.sendData(message);
            } catch (InterruptedException ie) {
                System.err.println("TCPSenderThread.java: " + ie.getMessage());
            } catch (IOException ioe) {
                System.err.println("TCPSenderThread.java: " + ioe.getMessage());
            }
        }
    }
}
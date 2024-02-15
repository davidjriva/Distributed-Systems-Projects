package csx55.overlay.transport;

import java.net.ServerSocket;
import csx55.overlay.node.Node;
import csx55.overlay.node.NodeInfo;

import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;

public class TCPServerThread implements Runnable {
    private volatile boolean isRunning; // stops server when needed.
    private ServerSocket serverSocket;
    private Node node;

    private int serverPort;

    private ConcurrentHashMap<String, NodeInfo> connectedNodes;

    public TCPServerThread(int port, Node node, ConcurrentHashMap<String, NodeInfo> connectedNodes) throws IOException {
        this.isRunning = true;
        this.serverSocket = new ServerSocket(port);
        this.serverPort = serverSocket.getLocalPort();
        this.node = node;
        this.connectedNodes = connectedNodes;
    }

    public int getServerPort() {
        return serverPort;
    }

    public void stopServer() {
        isRunning = false;
        try {
            serverSocket.close(); // Close the server socket to unblock the accept() call
        } catch (IOException e) {
            System.err.println("Error while closing the server socket: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        while (isRunning) {
            try {
                Socket incomingConnectionSocket = serverSocket.accept(); // Block --> Wait for client communications

                String hostName = incomingConnectionSocket.getInetAddress().getHostName(); 
                int portNum = incomingConnectionSocket.getPort();
                String ipAddress = incomingConnectionSocket.getRemoteSocketAddress().toString();

                TCPReceiverThread receiver = new TCPReceiverThread(incomingConnectionSocket, node);
                Thread thread = new Thread(receiver);
                thread.start();
            } catch (Exception e) {
                // Check if the thread was interrupted due to stopServer
                if (!isRunning) {
                    break;
                }
                // System.err.println(e.getMessage());
            }
        }
    }
}

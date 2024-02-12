package csx55.overlay.node;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import csx55.overlay.transport.*;
import csx55.overlay.util.Packet;
import csx55.overlay.wireformats.Event;
import csx55.overlay.wireformats.ConnectWithNeighborEvent;
import java.net.InetAddress;
import java.net.UnknownHostException;

public abstract class Node {
    private ConcurrentHashMap<String, NodeInfo> connectedNodes = new ConcurrentHashMap<>(); //Cache sockets this node has connected to
    private TCPServerThread serverThread;
    private TCPSenderThread senderThread;
    private int serverPort;

    public Node(int serverPort) {
        try {
            this.serverThread = new TCPServerThread(serverPort, this, connectedNodes);
            this.senderThread = new TCPSenderThread(this);
        } catch(IOException e) {
            System.err.println(e.getMessage());
        }

    }
    
    public void initializeServerPort() {
        this.serverPort = serverThread.getServerPort();
    }

    public void initializeServerThread() {
        Thread thread = new Thread(serverThread);
        thread.start();
    }
    
    public void initializeSenderThread() {
        Thread thread = new Thread(senderThread);
        thread.start();
    }

    public String generateKey(String hostName, int portNum) {
        return hostName + ":" + portNum;
    }

    public abstract void onEvent(Event e, Socket socket);

    public void createOneDirectionalConnection(String destination, int destinationPort) {
        try{
            InetAddress address = InetAddress.getByName(destination);

            String destinationHostName = address.getHostName();
            String destinationIpAddress = address.getHostAddress();

            NodeInfo destinationNode = new NodeInfo(destinationHostName, destinationIpAddress, destinationPort);

            //composite key = hostName:port
            connectedNodes.put(generateKey(destinationHostName, destinationPort), destinationNode); // cache connections

            Socket socket = destinationNode.getSocket();

            TCPReceiverThread receiver = new TCPReceiverThread(socket, this);
            Thread thread = new Thread(receiver);
            thread.start();
        }catch(UnknownHostException e) {
            System.err.println(e.getMessage());
        }catch(IOException e) {
            System.err.println(e.getMessage());
        }
    }

    public void createBidirectionalConnection(String destination, int destinationPort) {
        try{
            InetAddress address = InetAddress.getByName(destination);

            String destinationHostName = address.getHostName();
            String destinationIpAddress = address.getHostAddress();

            NodeInfo destinationNode = new NodeInfo(destinationHostName, destinationIpAddress, destinationPort);

            //composite key = hostName:port
            connectedNodes.put(generateKey(destinationHostName, destinationPort), destinationNode); // cache connections

            // Setup a receiver to listen on that socket
            Socket socket = destinationNode.getSocket();

            TCPReceiverThread receiver = new TCPReceiverThread(socket, this);
            Thread thread = new Thread(receiver);
            thread.start();

            // Send a request to the destination so that the destination can connect and cache this socket
            String localIpAddress = InetAddress.getLocalHost().getHostAddress();

            String key = destinationNode.getKey();
            ConnectWithNeighborEvent connectWithNeighborEvent = new ConnectWithNeighborEvent(localIpAddress, serverPort);
            Packet packet = new Packet(key, connectWithNeighborEvent.getBytes());

            getSenderThread().addToQueue(packet);
        }catch(UnknownHostException e) {
            System.err.println(e.getMessage());
        }catch(IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }

    public void sendEventToDestination(String destination, Event e) {
        try{
            Packet packet = new Packet(destination, e.getBytes());
            getSenderThread().addToQueue(packet);
        } catch (IOException ioe) {
            System.err.println("MessagingNodeEventHandler: Error transmitting bytes: " + ioe.getMessage());
        }
    }

    public void addConnectedNode(String key, NodeInfo nodeInfo) {
        connectedNodes.put(key, nodeInfo);
    }

    public ConcurrentHashMap<String, NodeInfo> getConnectedNodes() {
        return connectedNodes;
    }

    public TCPServerThread getTCPServerThread(){
        return serverThread;
    }
    
    public TCPSenderThread getSenderThread() {
        return senderThread;
    }

    public void setTCPServerThread(TCPServerThread serverThread){
        this.serverThread = serverThread;
    }
}

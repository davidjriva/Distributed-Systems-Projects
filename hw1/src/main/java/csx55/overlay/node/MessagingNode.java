package csx55.overlay.node;

import csx55.overlay.transport.*;
import csx55.overlay.wireformats.*;
import csx55.overlay.util.*;
import csx55.overlay.dijkstra.ShortestPathResult;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.io.IOException;
import java.util.Random;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MessagingNode extends Node {
    private String registryName;
    private int registryPort;

    private String hostName;
    private String ipAddress;
    private int serverPort;

    private MessagingNodeEventHandler messagingNodeEventHandler;

    private ConcurrentHashMap<String, ArrayList<String>> links; // stores <from, to--weight>    

    private StatTracker statTracker;

    public MessagingNode(String registryName, int registryPort) {
        super(0);
        this.registryName = registryName;
        this.registryPort = registryPort;

        this.links = new ConcurrentHashMap<>();
        this.messagingNodeEventHandler = new MessagingNodeEventHandler(this);

        this.statTracker = new StatTracker();

        try{
            this.ipAddress = InetAddress.getLocalHost().getHostAddress();
            this.hostName = InetAddress.getByName(ipAddress).getHostName();
            this.serverPort = getTCPServerThread().getServerPort();
        }catch(UnknownHostException e) {
            System.err.println(e.getMessage());
        }
    }

    public void onEvent(Event e, Socket socket){
        switch(e.getMessageType()) {
            case EventType.REGISTER_RESPONSE:
                messagingNodeEventHandler.handleRegistrationResponse((RegisterResponseEvent) e);
                break;
            case EventType.DEREGISTER_RESPONSE:
                messagingNodeEventHandler.handleDeregistrationResponse((DeregisterResponseEvent) e);
                break;
            case EventType.MESSAGING_NODES_LIST:
                messagingNodeEventHandler.handleMessagingNodesList((MessagingNodesListEvent)e);
                break;
            case EventType.CONNECT_WITH_NEIGHBOR:
                messagingNodeEventHandler.handleConnectWithNeighbor((ConnectWithNeighborEvent) e, socket);
                break;
            case EventType.LINK_WEIGHTS:
                messagingNodeEventHandler.handleLinkWeights((LinkWeightsEvent) e);
                break;
            case EventType.TASK_INITIATE:
                messagingNodeEventHandler.handleTaskInitiate((TaskInitiateEvent) e);
                break;
            case EventType.TRANSMIT_PAYLOAD:
                messagingNodeEventHandler.handleTransmitPayload((TransmitPayloadEvent) e);
                break;
            case EventType.TRAFFIC_SUMMARY_RESPONSE:
                messagingNodeEventHandler.handleTrafficSummaryResponse((TrafficSummaryResponseEvent) e);
                break;
            default:
                System.err.println("MessagingNode: Error! Hit end of onEvent()");
                throw new RuntimeException();
        }
    }

    public void registerNode(String registryName) {
        try {
            Event registerRequest = new RegisterRequestEvent(ipAddress, serverPort);

            String key = generateKey(registryName, registryPort);

            Packet packet = new Packet(key, registerRequest.getBytes());
            TCPSenderThread senderThread = getSenderThread();
            senderThread.addToQueue(packet);

        } catch (UnknownHostException e) {
            System.err.println("Error getting local host address: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error during registration: " + e.getMessage());
        }
    }

    public ConcurrentHashMap<String, ArrayList<String>> getLinks() {
        return links;
    }

    public String getRegistryName() {
        return registryName;
    }

    public int getRegistryPort() {
        return registryPort;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public int getServerPort() {
        return serverPort;
    }

    public String getHostName() {
        return hostName;
    }
    
    public synchronized StatTracker getStatTracker() {
        return statTracker;
    }

    // Returns a read-only copy of the shortest path map (thread-safe)
    public Map<String, ShortestPathResult> getShortestPaths() {
        return Collections.unmodifiableMap(messagingNodeEventHandler.getShortestPaths());
    }

    public static void main(String[] args){
        String registryName = args[0];
        int registryPort = Integer.parseInt(args[1]);

        // Register with a one directional connection to avoid prematurely getting 
        // added to the registered nodes list
        MessagingNode mn = new MessagingNode(registryName, registryPort);
        mn.initializeServerThread();
        mn.initializeSenderThread();
        mn.initializeServerPort();
        
        System.out.println("Server port= " + mn.getTCPServerThread().getServerPort());

        mn.createOneDirectionalConnection(registryName, registryPort);
        mn.registerNode(registryName);
        
        MessagingNodeCLI mnCLI = new MessagingNodeCLI(mn);
        mnCLI.runCLI();
    }
}

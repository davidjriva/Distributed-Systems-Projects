package csx55.overlay.node;

import csx55.overlay.transport.*;
import csx55.overlay.wireformats.*;
import csx55.overlay.util.StatTracker;
import csx55.overlay.dijkstra.*;
import java.util.Scanner;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.io.IOException;
import java.util.Random;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.HashSet;

public class MessagingNodeEventHandler {
    MessagingNode mn;
    Random random;

    public MessagingNodeEventHandler(MessagingNode mn) {
        this.mn = mn;
        this.random = new Random();
    }   

    public void handleLinkWeights(LinkWeightsEvent w){
        // Store the links
        for (String link : w.getLinks()){
            // transform this string formatted from:to weight into a Link object
            String[] parts = link.split(" ");

            String from = parts[0];
            String to = parts[1];
            String weight = parts[2];

            String value = to + "--" + weight;
            mn.getLinks().computeIfAbsent(from, k -> new ArrayList<>()).add(value);
        }

        System.out.println("Link weights received and processed. Ready to send messages.");
    }

    public void handleConnectWithNeighbor(ConnectWithNeighborEvent c, Socket socket){
        try{
            // cache connection
            String ipAddress = c.getIpAddress();

            String hostName = InetAddress.getByName(ipAddress).getHostName();
            int portNum = c.getPortNum();

            String key = mn.generateKey(hostName, portNum);
            mn.getConnectedNodes().put(key, new NodeInfo(hostName, ipAddress, portNum, socket));
        } catch (UnknownHostException e) {
            System.err.println(e.getMessage());
        }
    }

    public void handleMessagingNodesList(MessagingNodesListEvent m) {
        ArrayList<String> hostNamePortList = m.getHostNamePortList();

        for(String hostNamePort : hostNamePortList) {
            String[] parts = hostNamePort.split(":");
            String destination = parts[0];
            int destinationPort = Integer.parseInt(parts[1]);

            // Connect with the other messagingNode triggering a connectWithNeighborEvent
            mn.createBidirectionalConnection(destination, destinationPort);
        }

        System.out.println("All connections are established. Number of connections: " + m.getNumConnections());
    }

    public void handleRegistrationResponse(RegisterResponseEvent r) {
        //StatusCode 0 = Success, StatusCode 1 = Failure
        System.out.println(r.getAdditionalInfo());
    }

    public void handleDeregistrationResponse(DeregisterResponseEvent d) {
        System.out.println(d.getAdditionalInfo());

        try{
            // stop the server
            mn.getTCPServerThread().stopServer();
            mn.getSenderThread().stop();

            // close all remaining connections
            for (NodeInfo nodeInfo : mn.getConnectedNodes().values()) {
                nodeInfo.getSocket().close();
            }

            // exit and terminate the process
            System.exit(0);
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }

    public void handleTaskInitiate(TaskInitiateEvent t) {
        int rounds = t.getRounds();

        ConcurrentHashMap<String, ArrayList<String>> links = new ConcurrentHashMap<>(mn.getLinks());
        ConcurrentHashMap<String, ShortestPathResult> shortestPaths = new ConcurrentHashMap<>(); // cache for computed shortest paths

        try{
            // Get the host name of the current machine
            String ipAddress = mn.getIpAddress();
            String currHostName = InetAddress.getByName(ipAddress).getHostName();
                
            for (int i = 0; i < rounds; i++) {
                int numMessagesPerRound = 5; // CHANGE BASED ON REQUIREMENTS
                for (int j = 0; j < numMessagesPerRound; j++) {
                    // Find list of available nodes (excluding this node and choose a random one)
                    HashSet<String> availableNodes = new HashSet<>(links.keySet());
                    String currKey = currHostName + ":" + mn.getServerPort();

                    availableNodes.remove(currKey);

                    String randomNode = getRandomElement(availableNodes);
                    
                    String key =  currKey + "->" + randomNode;
                    // System.out.println("Routing a message: " + key);
                    
                    // Check cache for shortest path, if not found then compute and cache it.
                    ShortestPathResult shortestPath;
                    if (!shortestPaths.containsKey(key)){
                        // Compute and cache the path
                        DijkstraShortestPath djikstraShortestPath = new DijkstraShortestPath(links);
                        shortestPath = djikstraShortestPath.findShortestPath(currKey, randomNode);

                        shortestPaths.put(key, shortestPath);
                    } else {
                        shortestPath = shortestPaths.get(key);
                    }

                    // System.out.println("Shortest Path: ");
                    // System.out.println(shortestPath);
                    
                    ArrayList<String> path = new ArrayList<>(shortestPath.getPath());

                    // Get a number as payload
                    int payload = random.nextInt();

                    // Strip the first item from the path (this current node)
                    path.remove(0);
                    
                    // Create a transmit event and send it to the next element
                    TransmitPayloadEvent transmitPayloadEvent = new TransmitPayloadEvent(payload, path.size(), path);
                    String destination = path.get(0);

                    // Increment the sendTracker and sendSummation
                    mn.getStatTracker().incrementSendTracker();
                    mn.getStatTracker().addToSendSummation(payload);
                    
                    mn.sendEventToDestination(destination, transmitPayloadEvent);
                }
            }
        } catch (UnknownHostException e) {
            System.err.println(e.getMessage());
        }
        
        // Send a TASK_COMPLETE since all messages have been sent, relays will continue to happen in the background
        String hostName = mn.getHostName();
        int serverPort = mn.getServerPort(); 
        String key = mn.generateKey(hostName, serverPort);

        TaskCompleteEvent taskCompleteEvent = new TaskCompleteEvent(key);

        // Wait for all messages in queue to be sent before reporting completeness (mechanisms in place in registry ensures that it will requery for more current info if needed)
        while (mn.getSenderThread().getQueueSize() != 0) {
            try{
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                System.err.println(ie.getMessage());
            }
        }
        
        String registryName = mn.getRegistryName();
        int registryPort = mn.getRegistryPort();
        String registryKey = mn.generateKey(registryName, registryPort);

        mn.sendEventToDestination(registryKey, taskCompleteEvent);
    }

    public void handleTransmitPayload(TransmitPayloadEvent t) {
        if (t.getNumNodes() == 1){
            // Packet has arrived at its destination
            int payload = t.getPayload();

            mn.getStatTracker().incrementReceiveTracker();
            mn.getStatTracker().addToReceiveSummation(payload);
        } else {
            //Continue relaying the message
            // Increment relay count
            mn.getStatTracker().incrementRelayTracker();

            // Remove the current node from the path
            ArrayList<String> path = new ArrayList<>(t.getPath());
            path.remove(0);

            int payload = t.getPayload();
            TransmitPayloadEvent transmitPayloadEvent = new TransmitPayloadEvent(payload, path.size(), path);

            // Transmit the packet to the next node in the path
            String destination = path.get(0);
            mn.sendEventToDestination(destination, transmitPayloadEvent);
        }
    }

    private String getRandomElement(Set<String> set) {
        ArrayList<String> list = new ArrayList<>(set);

        int randomIndex = random.nextInt(list.size());

        return list.get(randomIndex);
    }

    public void handleTrafficSummaryResponse(TrafficSummaryResponseEvent t) {
        byte statusCode = t.getStatusCode();

        if (statusCode == EventType.TRAFFIC_SUMMARY_SUCCESS) {
            // Registry has received all messaging nodes stats from the system. 
            // Reset all counters
            mn.getStatTracker().resetAllCounters();
        } else {
            // Registry is querying for stats
            // Formulate a more current traffic summary response and send it to the registry
            StatTracker statTracker = mn.getStatTracker();

            String key = mn.generateKey(mn.getHostName(), mn.getServerPort());

            int sendTracker = statTracker.getSendTracker();
            int receiveTracker = statTracker.getReceiveTracker();
            int relayTracker = statTracker.getRelayTracker();
            long sendSummation = statTracker.getSendSummation();
            long receiveSummation = statTracker.getReceiveSummation();

            TrafficSummaryEvent nodeTrafficSummaryEvent = new TrafficSummaryEvent(key, sendTracker, receiveTracker, relayTracker, sendSummation, receiveSummation);
            String registryKey = mn.generateKey(mn.getRegistryName(), mn.getRegistryPort());

            mn.sendEventToDestination(registryKey, nodeTrafficSummaryEvent);
        }
    }
}
package csx55.overlay.util;

import csx55.overlay.wireformats.*;
import csx55.overlay.transport.TCPSender;
import csx55.overlay.dijkstra.Link;
import csx55.overlay.node.Registry;
import csx55.overlay.node.NodeInfo;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Random;
import java.util.HashSet;
import java.util.Set;
import java.io.IOException;

public class OverlayHandler {
    private ConcurrentHashMap<String, ArrayList<Link>> overlayConnections = new ConcurrentHashMap<>();

    private final Registry registry;

    private Random random;

    public OverlayHandler(Registry registry) {
        this.registry = registry;
        this.random = new Random(42);
    }

    public void setupOverlay(String input, ConcurrentHashMap<String, NodeInfo> connectedNodes) {
        String[] parts = input.split(" ");

        if (parts.length == 2) {
            try{
                // 1.) Read in an integer number following setup-overlay that is the number of connections
                int numConnections = Integer.parseInt(parts[1]);

                if(connectedNodes.size() < numConnections + 1) {    
                    // 2a.) Handle error condition where the number of messaging nodes is less than the connection limit specified
                    // Since a node cannot connect to itself, we should do +1 here
                    System.err.println("Registry: The number-of-connections specified is more than the available number of messaging nodes");
                }
                else if(numConnections <= 0){
                    System.err.println("Registry: The number-of-connections specified cannot be a negative number or zero");
                }else{
                    // 2b.) Create numConnections connections between nodes to each other 
                    // and maintain a count for each node of how many connections it has

                    // begin setting up the overlay by establishing circular connections between all the nodes 
                    buildCircularConnections();
                    
                    buildRemainingConnections(numConnections, connectedNodes);

                    // maps a messaging node to the messaging nodes hostname:ip that it will receive in its list
                    ConcurrentHashMap<String, ArrayList<String>> messagingNodesList = buildMessagingNodesList(); 

                    // transmits the messaging nodes list
                    for (String destinationKey : messagingNodesList.keySet()){
                        ArrayList<String> hostNameAndPortList = messagingNodesList.get(destinationKey);
                        sendMessagingNodesList(destinationKey, hostNameAndPortList);
                    }
                }
            } catch(NumberFormatException e) {
                System.err.println("Registry: Invalid number format. Please enter an integer following setup-overlay.");
            }
        } else{
            System.err.println("Registry: Invalid input format. Please specify: setup-overlay number-of-connections");
        }
    }

    public void sendMessagingNodesList(String destinationKey, ArrayList<String> hostNameAndPortList){
        try{
            MessagingNodesListEvent messagingNodesListEvent = new MessagingNodesListEvent(hostNameAndPortList.size(), hostNameAndPortList);
            Packet packet = new Packet(destinationKey, messagingNodesListEvent.getBytes());
            registry.getSenderThread().addToQueue(packet);
        } catch(IOException e) {
            System.err.println(e.getMessage());
        }
    }

    public ConcurrentHashMap<String, ArrayList<String>> buildMessagingNodesList() {
        // Copy the overlay connections to not modify the original list
        ConcurrentHashMap<String, ArrayList<Link>> overlayConnectionsCopy = new ConcurrentHashMap<>(overlayConnections);

        ConcurrentHashMap<String, ArrayList<String>> messagingNodesList = new ConcurrentHashMap<>();

        // Populate messagingNodesList arrayList
        for (String key : overlayConnectionsCopy.keySet()) {
            messagingNodesList.put(key, new ArrayList<>());
            for (Link value : overlayConnectionsCopy.get(key)) {
                messagingNodesList.get(key).add(value.getTo().getKey());
            }
        }

        for (String from : messagingNodesList.keySet()) {
            for (String to : messagingNodesList.get(from)) {
                messagingNodesList.get(to).remove(from);
            }
        }
        
        return messagingNodesList;
    }

    public void clearOverlay() {
        this.overlayConnections = new ConcurrentHashMap<>();
    }


    public void addLink(String key, Link link) {
        ArrayList<Link> existingLinks = overlayConnections.getOrDefault(key, new ArrayList<>());
        existingLinks.add(link);
        overlayConnections.put(key, existingLinks);

        // Add the reverse link as well
        String reverseKey = link.getTo().getKey();
        ArrayList<Link> reverseExistingLinks = overlayConnections.getOrDefault(reverseKey, new ArrayList<>());
        reverseExistingLinks.add(new Link(link.getTo(), link.getFrom(), link.getWeight()));
        overlayConnections.put(reverseKey, reverseExistingLinks);
    }

    public void buildCircularConnections() {
        ConcurrentHashMap<String, NodeInfo> connectedNodes = registry.getConnectedNodes();
        
        int numNodes = connectedNodes.size();

        int loopNum = numNodes;

        // Handle edge case with two nodes creating 1 link
        if (numNodes == 2) {
            loopNum -= 1;
        }
        
        for (int i = 0; i < loopNum; i++) {
            NodeInfo currentNode = new ArrayList<>(connectedNodes.values()).get(i);
            NodeInfo nextNode = new ArrayList<>(connectedNodes.values()).get((i + 1) % numNodes);

            int weight = random.nextInt(10) + 1;
            Link link = new Link(currentNode, nextNode, weight);

            String key = currentNode.getKey();

            addLink(key, link);
        }
    }

    public void buildRemainingConnections(int numConnections, ConcurrentHashMap<String, NodeInfo> connectedNodes){
        Set<String> usedNodes = new HashSet<>();

        for (String key : overlayConnections.keySet()) {
            NodeInfo currentNode = overlayConnections.get(key).get(0).getFrom();
            usedNodes.add(currentNode.getKey());
        }

        for (String key : overlayConnections.keySet()) {
            //TODO: RESUME HERE?
            NodeInfo currentNode = overlayConnections.get(key).get(0).getFrom();
            buildConnectionsForNode(currentNode, usedNodes, numConnections, connectedNodes);
        }
    }

    private void buildConnectionsForNode(NodeInfo currentNode, Set<String> usedNodes, int numConnections, ConcurrentHashMap<String, NodeInfo> connectedNodes) {
        while (overlayConnections.get(currentNode.getKey()).size() < numConnections) {
            String targetNodeKey = getRandomNodeKey(currentNode, usedNodes);

            if (targetNodeKey == null) {
                break; // No more available nodes
            }

            NodeInfo targetNode = connectedNodes.get(targetNodeKey);

            // Check if the link already exists
            if (!isLinkExists(currentNode, targetNode)) {
                int weight = random.nextInt(10) + 1;
                Link link = new Link(currentNode, targetNode, weight);

                String key = currentNode.getKey();
                addLink(key, link);
                usedNodes.add(targetNodeKey);
            }
        }
    }

    private boolean isLinkExists(NodeInfo currentNode, NodeInfo targetNode) {
        ArrayList<Link> existingLinks = overlayConnections.getOrDefault(currentNode.getKey(), new ArrayList<>());

        for (Link link : existingLinks) {
            if (link.getFrom().equals(currentNode) && link.getTo().equals(targetNode)) {
                return true;
            }
        }

        return false;
    }

    private String getRandomNodeKey(NodeInfo currentNode, Set<String> usedNodes) {
        ArrayList<String> availableNodes = new ArrayList<>(usedNodes);

        availableNodes.remove(currentNode.getKey());

        if (availableNodes.isEmpty()) {
            return null;
        }

        int randomIndex = random.nextInt(availableNodes.size());
        return availableNodes.get(randomIndex);
    }

    public void sendOverlayLinkWeights() {
        // Transform link weights into format from:to weight
        try{
            ArrayList<String> transmitLinks = new ArrayList<>();

            for (ArrayList<Link> links : overlayConnections.values()){
                for (Link link : links) {
                    String formattedLink = link.getTo().getKey() + " " + link.getFrom().getKey() + " " + link.getWeight();
                    transmitLinks.add(formattedLink);
                }
            }

            // Create event for transmission
            LinkWeightsEvent linkWeightsEvent = new LinkWeightsEvent(transmitLinks.size(), transmitLinks);

            // Send event to all messaging nodes
            for(NodeInfo nodeInfo : registry.getConnectedNodes().values()){
                String key = nodeInfo.getKey();
                Packet packet = new Packet(key, linkWeightsEvent.getBytes());
                registry.getSenderThread().addToQueue(packet);
            }
        }catch(IOException ioe){
            System.err.println(ioe.getMessage());
        }
    }

    public void sendStartCommand(String input) {
        String[] parts =  input.split(" ");

        if(parts.length == 2){
            int numRounds = -1;
            try{
                numRounds = Integer.parseInt(parts[1]);

                if(numRounds <= 0){
                    System.out.println("Registry: numRounds should be greater than or equal to zero");
                } else {
                    TaskInitiateEvent taskInitiateEvent = new TaskInitiateEvent(numRounds);
                    registry.sendMessageToAllNodes(taskInitiateEvent);
                }
            }catch(NumberFormatException nfe){
                System.err.println(nfe.getMessage() + ". Input must be a valid number");
            }
        } else {
            System.err.println("Registry: Please specify start number-of-rounds");
        }
    }

    public void listWeights() {
        for (ArrayList<Link> links : overlayConnections.values()) {
            for (Link link : links) {
                System.out.println(link.toString());
            }
        }
    }
}
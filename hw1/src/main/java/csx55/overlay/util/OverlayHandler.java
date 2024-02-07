package csx55.overlay.util;

import csx55.overlay.wireformats.*;
import csx55.overlay.transport.TCPSender;
import csx55.overlay.dijkstra.Link;
import csx55.overlay.node.Registry;
import csx55.overlay.node.NodeInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Random;
import java.util.HashSet;
import java.util.Set;
import java.io.IOException;
import java.util.Map;

public class OverlayHandler {
    private ConcurrentHashMap<String, ArrayList<Link>> overlayConnections = new ConcurrentHashMap<>();

    private final Registry registry;

    private Random random;

    public OverlayHandler(Registry registry) {
        this.registry = registry;
        this.random = new Random(42);
    }

    public void setupOverlay(String input, Map<String, NodeInfo> connectedNodes) {
        String[] parts = input.split(" ");

        if (parts.length == 2) {
            // 1.) Read in an integer number following setup-overlay that is the number of connections
            int numConnections = Integer.MIN_VALUE;
            try{
                numConnections = Integer.parseInt(parts[1]);
            } catch(NumberFormatException e) {
                System.err.println("Registry: Invalid number format. Please enter an integer following setup-overlay.");
                return;
            }

            if(connectedNodes.size() < numConnections + 1) {    
                // 2a.) Handle error condition where the number of messaging nodes is less than the connection limit specified
                // Since a node cannot connect to itself, we should do +1 here
                System.err.println("Registry: The number-of-connections specified is more than the available number of messaging nodes");
                return;
            }
            else if(numConnections <= 1){
                System.err.println("Registry: The number-of-connections specified must be greater than 1");
                return;
            }else{
                buildConnections(numConnections, connectedNodes);

                // maps a messaging node to the messaging nodes hostname:ip that it will receive in its list
                ConcurrentHashMap<String, ArrayList<String>> messagingNodesList = buildMessagingNodesList(); 

                // transmits the messaging nodes list
                for (String destinationKey : messagingNodesList.keySet()){
                    ArrayList<String> hostNameAndPortList = messagingNodesList.get(destinationKey);
                    sendMessagingNodesList(destinationKey, hostNameAndPortList);
                }
            }
        } else{
            System.err.println("Registry: Invalid input format. Please specify: setup-overlay number-of-connections");
            return;
        }
    }

    private void buildConnections(int numConnections, Map<String, NodeInfo> connectedNodes) {
        List<NodeInfo> availableNodes = new ArrayList<>(connectedNodes.values());
        if (numConnections % 2 == 0) {
            // even case
            buildEvenConnections(numConnections, availableNodes);
        } else {
            // odd case
            buildOpposingConnections(availableNodes);
            buildEvenConnections(numConnections - 1, availableNodes);
        }
    }

    public void buildOpposingConnections(List<NodeInfo> availableNodes) {
        // Adds a link to the point opposite in the circle
        for (int i = 0; i < availableNodes.size() / 2; i++) {
            NodeInfo currNode = availableNodes.get(i); // current node
            NodeInfo neighbor = availableNodes.get((i + (availableNodes.size() / 2)));
            int weight = random.nextInt(10) + 1;

            Link link = new Link(currNode, neighbor, weight);

            addLink(currNode.getKey(), link);
        }
    }

    public void buildEvenConnections(int numConnections, List<NodeInfo> availableNodes) {
        for (int i = 0; i < availableNodes.size(); i++) {
            for (int j = 1; j <= numConnections / 2; j++) {
                NodeInfo currNode = availableNodes.get(i); // current node
                NodeInfo neighbor = availableNodes.get((i + j) % availableNodes.size());
                int weight = random.nextInt(10) + 1;

                Link link = new Link(currNode, neighbor, weight); // (from, to, weight)

                addLink(currNode.getKey(), link);
            }
        }
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

    public void sendOverlayLinkWeights() {
        // Transform link weights into format from:to weight
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
        registry.sendMessageToAllNodes(linkWeightsEvent);     
    }

    public void sendStartCommand(String input) {
        String[] parts =  input.split(" ");

        if(parts.length == 2){
            try{
                numRounds = Integer.parseInt(parts[1]);
            }catch(NumberFormatException nfe){
                System.err.println(nfe.getMessage() + ". Input must be a valid number");
            }

            if(numRounds <= 0){
                System.out.println("Registry: numRounds should be greater than or equal to zero");
            } else {
                TaskInitiateEvent taskInitiateEvent = new TaskInitiateEvent(numRounds);
                registry.sendMessageToAllNodes(taskInitiateEvent);
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
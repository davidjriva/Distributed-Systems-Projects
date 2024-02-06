package csx55.overlay.node;

import csx55.overlay.wireformats.DeregisterRequestEvent;
import csx55.overlay.transport.TCPSender;
import csx55.overlay.util.Packet;
import csx55.overlay.dijkstra.ShortestPathResult;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessagingNodeCLI {
    MessagingNode mn;

    public MessagingNodeCLI(MessagingNode mn) {
        this.mn = mn;
    }

    public void runCLI() {
        Scanner scanner = new Scanner(System.in);
        String input = "";

        while(true){
            input = scanner.nextLine();

            if(input.equals("exit-overlay")){
                exitOverlay();
            } else if (input.equals("list-messaging nodes")){
                for (NodeInfo n : mn.getConnectedNodes().values()){
                    n.printNodeInfo();
                }
            } else if (input.equals("list-weights")){
                listWeights();
            } else if (input.equals("list-stats")) {
                System.out.println(mn.getStatTracker());
            } else if (input.equals("print-shortest-path")) {
                Map<String, ShortestPathResult> shortestPaths = mn.getShortestPaths();
                ConcurrentHashMap<String, ArrayList<String>> links = new ConcurrentHashMap<>(mn.getLinks()); // Format: <node from, [node to 1 name -- weight, node to 2 name -- weight, node to 3 name -- weight]

                for (ShortestPathResult spResult : shortestPaths.values()) {
                    ArrayList<String> path = spResult.getPath();
                    String fromNode;
                    String toNode;
                    for (int i = 0; i < path.size(); i++){
                        if (i == path.size() - 1) {
                            System.out.print(path.get(i) + "\n");
                        } else {
                            fromNode = path.get(i);
                            toNode = path.get(i + 1);
                            int weight = Integer.MIN_VALUE;

                            for (String link : links.get(fromNode)){
                                String[] parts = link.split("--");

                                if (parts[0].equals(toNode)){
                                    weight = Integer.parseInt(parts[1]);
                                    break;
                                } 
                            }

                            System.out.print(path.get(i) + "--" + weight + "--");
                        }
                    }
                }
            }
        }
    }

    public void listWeights() {
        ConcurrentHashMap<String, ArrayList<String>> links = mn.getLinks();
        for (String from : links.keySet()) {
            for (String link : links.get(from)) {
                System.out.println(from + " " + link);
            }
        }
    }

    public void exitOverlay() {
        String registryName = mn.getRegistryName();
        int registryPort = mn.getRegistryPort();

        String ipAddress = mn.getIpAddress();
        int serverPort = mn.getServerPort();

        try{
           //Send deregister request
            String key = mn.generateKey(registryName, registryPort);
            NodeInfo registryNode = mn.getConnectedNodes().get(key);

            DeregisterRequestEvent deregisterRequest = new DeregisterRequestEvent(ipAddress, serverPort);
            
            Packet packet = new Packet(key, deregisterRequest.getBytes());
            mn.getSenderThread().addToQueue(packet);
        } catch (UnknownHostException e) {
            System.err.println(e.getMessage());
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }
}
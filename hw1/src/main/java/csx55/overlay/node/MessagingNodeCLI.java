package csx55.overlay.node;

import java.util.Scanner;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import csx55.overlay.wireformats.DeregisterRequestEvent;
import csx55.overlay.transport.TCPSender;

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
            
            TCPSender sender = registryNode.getSender();
            sender.sendData(deregisterRequest.getBytes());
        } catch (UnknownHostException e) {
            System.err.println(e.getMessage());
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }
}
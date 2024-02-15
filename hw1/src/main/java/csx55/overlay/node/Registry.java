package csx55.overlay.node;

import java.util.Scanner;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import csx55.overlay.transport.TCPSender;
import csx55.overlay.wireformats.*;
import csx55.overlay.util.*;

public class Registry extends Node {
    private final RegistrationHandler registrationHandler;
    private final DeregistrationHandler deregistrationHandler;
    
    private final OverlayHandler overlayHandler;

    public OverlayHandler getOverlayHandler() {
        return overlayHandler;
    }

    public Registry(int port) {
        super(port);
        this.registrationHandler = new RegistrationHandler(this);
        this.deregistrationHandler = new DeregistrationHandler(this);
        this.overlayHandler = new OverlayHandler(this);
    }

    public void onEvent(Event e, Socket socket){
        switch(e.getMessageType()) {
            case EventType.REGISTER_REQUEST:
                registrationHandler.handleRegistrationRequest((RegisterRequestEvent) e, socket);
                break;
            case EventType.DEREGISTER_REQUEST:
                deregistrationHandler.handleDeregistrationRequest((DeregisterRequestEvent) e, socket);
                break;
            case EventType.TASK_COMPLETE:
                handleTaskComplete((TaskCompleteEvent) e);
                break;
            case EventType.TRAFFIC_SUMMARY:
                handleTrafficSummary((TrafficSummaryEvent) e);
                break;
            default:
                throw new RuntimeException();
        }
    }

    public void handleTaskComplete(TaskCompleteEvent t){
        // Send a TRAFFIC_SUMMARY event to all messaging nodes to get a response containing their traffic info
        TrafficSummaryResponseEvent trafficSummaryResponse = new TrafficSummaryResponseEvent((byte) 1);
        String nodeKey = t.getNodeKey();

        sendEventToDestination(nodeKey, trafficSummaryResponse);
    }   

    AtomicInteger summariesReceived = new AtomicInteger(0);
    List<TrafficSummaryEvent> trafficSummaryEvents = Collections.synchronizedList(new ArrayList<>());
    
    AtomicInteger consecutiveRoundsWithNoChange = new AtomicInteger(0);
    AtomicInteger prevTotalMessagesSent = new AtomicInteger(0);
    AtomicInteger prevTotalMessagesReceived = new AtomicInteger(0);

    public synchronized void handleTrafficSummary(TrafficSummaryEvent t) {
        // Increment traffic summary counter and store this event
        summariesReceived.getAndIncrement();
        trafficSummaryEvents.add(t);

        if (summariesReceived.get() == getConnectedNodes().size()) {
            synchronized(trafficSummaryEvents) {
                int totalMessagesSent = 0;
                int totalMessagesReceived = 0;
                int expectedMessagesSent = getConnectedNodes().size() * 5 * overlayHandler.getNumRounds();
                
                for (TrafficSummaryEvent tse : trafficSummaryEvents) {
                    totalMessagesReceived += tse.getReceiveTracker();
                    totalMessagesSent += tse.getSendTracker();
                }

                boolean noChangeThisRound = (totalMessagesSent == prevTotalMessagesSent.get() && totalMessagesReceived == prevTotalMessagesReceived.get());

                if (noChangeThisRound) {
                    consecutiveRoundsWithNoChange.incrementAndGet();

                    // Handles case where packets are lost, so a table prints at least
                    if (consecutiveRoundsWithNoChange.get() == 3) {
                        System.out.println("There has been no change in the past three TRAFFIC_SUMMARY queries.");
                        communicateTrafficSummarySuccess();
                        printTrafficSummaryTable();
                    }
                } else if (totalMessagesSent != expectedMessagesSent || totalMessagesSent != totalMessagesReceived) {
                    requeryForTrafficSummaries();
                } else {
                    communicateTrafficSummarySuccess();
                    printTrafficSummaryTable();
                }

                prevTotalMessagesSent.set(totalMessagesSent);
                prevTotalMessagesReceived.set(totalMessagesReceived);

                summariesReceived.set(0);
                trafficSummaryEvents.clear();
            }
        }
    }

    private void requeryForTrafficSummaries() {
        // Requery the nodes for more current information
        TrafficSummaryResponseEvent trafficSummaryResponse = new TrafficSummaryResponseEvent(EventType.TRAFFIC_SUMMARY_FAILURE);
        sendMessageToAllNodes(trafficSummaryResponse);
    }

    private void communicateTrafficSummarySuccess() {
        // Success, printing out the correct TRAFFIC_SUMMARY
        consecutiveRoundsWithNoChange.set(0);
        prevTotalMessagesSent.set(0);
        prevTotalMessagesReceived.set(0);
    
        // Send TRAFFIC_SUMMARY_RESPONSE event to signal all messaging nodes to clear their statTrackers
        TrafficSummaryResponseEvent trafficSummaryResponse = new TrafficSummaryResponseEvent(EventType.TRAFFIC_SUMMARY_SUCCESS);
        sendMessageToAllNodes(trafficSummaryResponse);
    }

    private void printTrafficSummaryTable() {
        synchronized(trafficSummaryEvents){
            // Print table header
            System.out.printf("%-40s %-30s %-30s %-30s %-30s %-30s%n", "MessagingNode Name", "Number of messages sent", "Number of messages received",
                "Summation of sent messages", "Summation of received messages", "Number of messages relayed");

            // Iterate and print each individual messagingNode's stats
            Iterator<TrafficSummaryEvent> i = trafficSummaryEvents.iterator();

            int totalMessagesSent = 0;
            int totalMessagesReceived = 0;
            long totalSentSum = 0;
            long totalReceivedSum = 0;
            int totalMessagesRelayed = 0;

            while (i.hasNext()) {
                TrafficSummaryEvent tmp = i.next();
                System.out.printf("%-40s %-30d %-30d %-30d %-30d %-30d%n", tmp.getNodeKey(), tmp.getSendTracker(), tmp.getReceiveTracker(), tmp.getSendSummation(),
                tmp.getReceiveSummation(), tmp.getRelayTracker());

                totalMessagesSent += tmp.getSendTracker();
                totalMessagesReceived += tmp.getReceiveTracker();
                totalSentSum += tmp.getSendSummation();
                totalReceivedSum += tmp.getReceiveSummation();
                totalMessagesRelayed += tmp.getRelayTracker();
            }

            // Print the final summation
            System.out.printf("%-40s %-30d %-30d %-30d %-30d %-30d%n", "Sum", totalMessagesSent, totalMessagesReceived, totalSentSum,
            totalReceivedSum, totalMessagesRelayed);
        }
    }

    public void sendMessageToAllNodes(Event e) {
        try{
            for(NodeInfo nodeInfo : getConnectedNodes().values()){
                String key = nodeInfo.getKey();
                Packet packet = new Packet(key, e.getBytes());
                getSenderThread().addToQueue(packet);
            }
        } catch (IOException ioe) {
            System.err.println("Registry: Error sending message to all nodes: " + ioe.getMessage());
        }
    }

    public static void main(String[] args) {    
        int portNum = Integer.parseInt(args[0]);
        Registry registry = new Registry(portNum);
        registry.initializeServerThread();
        registry.initializeSenderThread();
        registry.initializeServerPort();

        // System.out.println("Server port= " + args[0]);

        RegistryCLI registryCLI = new RegistryCLI(registry, registry.overlayHandler);
        registryCLI.runCLI();
    }
}
package csx55.overlay.util;

import csx55.overlay.wireformats.*;
import csx55.overlay.node.*;
import csx55.overlay.transport.*;
import csx55.overlay.util.Packet;
import java.net.Socket;
import java.io.IOException;
import java.util.Scanner;

public class DeregistrationHandler {
    public static final String DEREGISTER_SUCCESS = "Deregistration request successful. The number of messaging nodes currently constituting the overlay is now (%d)";
    private static final String UNREGISTERED_NODE_ATTEMPTING_DEREGISTER_MESSAGE = "Deregistration request unsuccessful. This node(%s) has not yet registered with the registry";
    private static final String MISMATCH_IP_ADDRESS_MESSAGE = "Deregistration request unsuccessful. Mismatch in IP address provided(%s) and the one associated with this socket(%s)";

    private final Registry registry;

    public DeregistrationHandler(Registry registry) {
        this.registry = registry;
    }

    public void handleDeregistrationRequest(DeregisterRequestEvent request, Socket socket) {
        String hostName = socket.getInetAddress().getHostName();
        int portNum = request.getPortNum();

        String key = registry.generateKey(hostName, portNum);
        
        NodeInfo nodeInfo = registry.getConnectedNodes().get(key);

        RegistrationResult result = validateDeregistration(request.getIpAddress(), socket, nodeInfo, hostName, portNum);

        sendDeregistrationResponse(result.getStatusCode(), result.getAdditionalInfo(), nodeInfo, hostName, portNum);
    }

    public RegistrationResult validateDeregistration(String requestIpAddress, Socket socket, NodeInfo nodeInfo, String hostName, int portNum) {
        String key = hostName + ":" + portNum;

        if(nodeInfo == null) {
            //nodeInfo is null for unregistered nodes as they are not in the registry's connected node list
            return new RegistrationResult(EventType.DEREGISTER_FAILURE, String.format(UNREGISTERED_NODE_ATTEMPTING_DEREGISTER_MESSAGE, key));
        }else if (!nodeInfo.getIpAddress().equals(requestIpAddress)) {
            return new RegistrationResult(EventType.DEREGISTER_FAILURE, String.format(MISMATCH_IP_ADDRESS_MESSAGE, requestIpAddress, nodeInfo.getIpAddress()));
        } else {
            // subtract 1 to account for the node that is removed after this message gets sent
            return new RegistrationResult(EventType.DEREGISTER_SUCCESS, String.format(DEREGISTER_SUCCESS, registry.getConnectedNodes().size() - 1));
        }
    }

    public void sendDeregistrationResponse(byte statusCode, String additionalInfo, NodeInfo nodeInfo, String hostName, int portNum) {
        try {
            Event deregisterResponse = new DeregisterResponseEvent(statusCode, additionalInfo);
            
            if (nodeInfo == null) {
                // Need to communicate with an unregistered node
                Socket socket = new Socket(hostName, portNum);
                TCPSender tempSender = new TCPSender(socket);
                tempSender.sendData(deregisterResponse.getBytes());
                tempSender.closeSender();
            }else {
                String key = nodeInfo.getKey();
                
                Packet packet = new Packet(key, deregisterResponse.getBytes());
                TCPSenderThread senderThread = registry.getSenderThread();
                senderThread.addToQueue(packet);
                
                // Avoid modifying the connected nodes list prior to sending the deregistration response
                while (senderThread.getQueueSize() != 0) {
                    try{
                        Thread.sleep(10);
                    } catch (InterruptedException ie) {
                        System.err.println("DeregistrationHandler.java: " + ie.getMessage());
                    }
                }

                registry.getConnectedNodes().remove(key);
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}
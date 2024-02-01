package csx55.overlay.node;

import java.util.Scanner;
import csx55.overlay.wireformats.Event;
import csx55.overlay.wireformats.RegisterRequestEvent;
import csx55.overlay.wireformats.RegisterResponseEvent;
import csx55.overlay.wireformats.DeregisterRequestEvent;
import csx55.overlay.wireformats.DeregisterResponseEvent;
import csx55.overlay.wireformats.RegistrationResult;
import csx55.overlay.transport.TCPSender;
import csx55.overlay.util.Packet;
import java.net.Socket;
import csx55.overlay.wireformats.EventType;
import csx55.overlay.node.NodeInfo;
import java.io.IOException;

public class RegistrationHandler {
    private static final String REGISTRATION_SUCCESS_MESSAGE = "Registration request successful. The number of messaging nodes currently constituting the overlay is (%d)";
    private static final String MISMATCH_IP_ADDRESS_MESSAGE = "Registration request unsuccessful. Mismatch in IP address provided(%s) and the one associated with this socket(%s)";
    private static final String ALREADY_REGISTERED_MESSAGE = "Registration request unsuccessful. %s is already registered with the registry";

    private final Registry registry;

    public RegistrationHandler(Registry registry) {
        this.registry = registry;
    }

    public void handleRegistrationRequest(RegisterRequestEvent request, Socket socket) {
        String remoteIpAddress = request.getIpAddress();
        String remoteHostName = socket.getInetAddress().getHostName();
        int remoteServerPortNum = request.getPortNum();

        NodeInfo nodeInfo = new NodeInfo(remoteHostName, remoteIpAddress, remoteServerPortNum, socket);

        RegistrationResult result = validateRegistration(socket.getInetAddress().getHostAddress(), socket, nodeInfo);

        // Send REGISTER_RESPONSE event to the MessagingNode
        sendRegistrationResponse(result.getStatusCode(), result.getAdditionalInfo(), nodeInfo);
    }

    private RegistrationResult validateRegistration(String socketIpAddress, Socket socket, NodeInfo nodeInfo) {
        String hostName = nodeInfo.getHostName();
        int portNum = nodeInfo.getPortNum();
        String key = registry.generateKey(hostName, portNum);

        if (!nodeInfo.getIpAddress().equals(socketIpAddress)) {
            return new RegistrationResult(EventType.REGISTER_FAILURE, String.format(MISMATCH_IP_ADDRESS_MESSAGE, nodeInfo.getIpAddress(), socketIpAddress));
        } else if (registry.getConnectedNodes().containsKey(key)) {
            return new RegistrationResult(EventType.REGISTER_FAILURE, String.format(ALREADY_REGISTERED_MESSAGE, key));
        } else {
            // Cache for future use
            registry.addConnectedNode(key, nodeInfo);

            // Update info field for success
            String additionalInfo = String.format(REGISTRATION_SUCCESS_MESSAGE, registry.getConnectedNodes().size());
            return new RegistrationResult(EventType.REGISTER_SUCCESS, additionalInfo);
        }
    }

    public void sendRegistrationResponse(byte statusCode, String additionalInfo, NodeInfo nodeInfo) {
        try{
            String key = nodeInfo.getKey();
            Event registerResponse = new RegisterResponseEvent(statusCode, additionalInfo);
            Packet packet = new Packet(key, registerResponse.getBytes());

            registry.getSenderThread().addToQueue(packet);
        } catch(IOException e) {
            // remove the messagingNode if connection is lost
            String hostName = nodeInfo.getHostName();
            int portNum = nodeInfo.getPortNum();
            String key = registry.generateKey(hostName, portNum);

            while (registry.getSenderThread().getQueueSize() != 0) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {
                    System.err.println("RegistrationHandler.java: " + ie.getMessage());
                }
            }   

            registry.getConnectedNodes().remove(key);
            System.err.println(e.getMessage());
        }
    }
}
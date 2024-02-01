package csx55.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.DataOutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.net.Socket;


public class EventFactory {
    // switch on message type and return correct Event object
    // make each message type its own class
    public static Event createEvent(byte[] data, Socket socket) {
        try{
            ByteArrayInputStream baInputStream = new ByteArrayInputStream(data);
            DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

            // Removes message type header
            int messageType = din.readInt();

            // System.out.println("[" + Thread.currentThread().getId() + "] Received message of type: " + messageType);

            // Read in all the data EXCEPT for the first byte that is messageType and pass that on to the next Event object
            byte[] remainingData = new byte[data.length - Integer.BYTES];
            din.readFully(remainingData);

            switch (messageType) {
                case EventType.REGISTER_REQUEST:
                    return new RegisterRequestEvent(remainingData);
                case EventType.REGISTER_RESPONSE:
                    return new RegisterResponseEvent(remainingData);
                case EventType.DEREGISTER_REQUEST:
                    return new DeregisterRequestEvent(remainingData);
                case EventType.DEREGISTER_RESPONSE:
                    return new DeregisterResponseEvent(remainingData);
                case EventType.CONNECT_WITH_NEIGHBOR:
                    return new ConnectWithNeighborEvent(remainingData);
                case EventType.MESSAGING_NODES_LIST:
                    return new MessagingNodesListEvent(remainingData);
                case EventType.LINK_WEIGHTS:
                    return new LinkWeightsEvent(remainingData);
                case EventType.TASK_INITIATE:
                    return new TaskInitiateEvent(remainingData);
                case EventType.TRANSMIT_PAYLOAD:
                    return new TransmitPayloadEvent(remainingData);
                case EventType.TASK_COMPLETE:
                    return new TaskCompleteEvent(remainingData);
                case EventType.TRAFFIC_SUMMARY:
                    return new TrafficSummaryEvent(remainingData);
                case EventType.TRAFFIC_SUMMARY_RESPONSE:
                    return new TrafficSummaryResponseEvent(remainingData);
                default:
                    System.err.println("EventFactory: Error! "+ "[" + Thread.currentThread().getId() + "] Reached end of switch statement with messagetype: " + messageType + " over socket " + socket);
                    throw new RuntimeException();
            }
        } catch (IOException e) {
            System.err.println("EventFactory: " + "[" + Thread.currentThread().getId() + "]IO Exception");
            e.printStackTrace();
        }

        System.err.println("Error: Reached end of EventFactory!");
        return null;
    }
}
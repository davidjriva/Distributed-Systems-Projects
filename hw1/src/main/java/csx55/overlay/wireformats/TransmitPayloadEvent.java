package csx55.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.DataOutputStream;
import java.util.ArrayList;

public class TransmitPayloadEvent implements Event {
    int payload;

    int numNodes; 
    ArrayList<String> path;    
    
    public TransmitPayloadEvent(int payload, int numNodes, ArrayList<String> path) {
        if (numNodes <= 0) {
            throw new IllegalArgumentException("TransmitPayloadEvent: The number of nodes should never be zero or negative!");
        }

        this.payload = payload;
        this.numNodes = numNodes;
        this.path = path;
    }

    public TransmitPayloadEvent(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
        
        this.payload = din.readInt();

        this.numNodes = din.readInt();

        this.path = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            int elementLength = din.readInt();
            byte[] nodeBytes = new byte[elementLength];
            din.readFully(nodeBytes);
            String node = new String(nodeBytes);
            path.add(node);
        }

        baInputStream.close();
        din.close();
    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledBytes = null;

        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dout.writeInt(getMessageType());

        dout.writeInt(payload);

        dout.writeInt(numNodes);

        for (String node : path) {
            byte[] nodeBytes = node.getBytes();
            int elementLength = nodeBytes.length;
            dout.writeInt(elementLength);
            dout.write(nodeBytes);
        }

        dout.flush();

        marshalledBytes = baOutputStream.toByteArray();

        baOutputStream.close();
        dout.close();
        return marshalledBytes;
    }

        
    public void printEventInfo() {
        System.out.println("MessageType: " + getMessageType() + ", payload: " + payload + ", numNodes: " + numNodes + ", path: " + path);
    }

    public int getMessageType() {
        return EventType.TRANSMIT_PAYLOAD;
    }

    public int getPayload() {
        return payload;
    }

    public int getNumNodes() {
        return numNodes;
    }

    public ArrayList<String> getPath() {
        return path;
    }
}

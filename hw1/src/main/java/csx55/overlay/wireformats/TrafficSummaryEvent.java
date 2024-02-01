package csx55.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.DataOutputStream;

public class TrafficSummaryEvent implements Event {
    String nodeKey;

    int sendTracker;
    int receiveTracker;
    int relayTracker;

    long sendSummation;
    long receiveSummation;

    public TrafficSummaryEvent(String nodeKey, int sendTracker, int receiveTracker, int relayTracker, long sendSummation, long receiveSummation) {
        this.nodeKey = nodeKey;
        this.sendTracker = sendTracker;
        this.receiveTracker = receiveTracker;
        this.relayTracker = relayTracker;
        this.sendSummation = sendSummation;
        this.receiveSummation = receiveSummation;
    }

    public TrafficSummaryEvent(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        int nodeKeyLength = din.readInt();
        byte[] nodeKeyBytes = new byte[nodeKeyLength];
        din.readFully(nodeKeyBytes);
        this.nodeKey = new String(nodeKeyBytes);

        this.sendTracker = din.readInt();
        this.receiveTracker = din.readInt();
        this.relayTracker = din.readInt();

        this.sendSummation = din.readLong();
        this.receiveSummation = din.readLong();

        baInputStream.close();
        din.close();
    }   

    public byte[] getBytes() throws IOException {
        byte[] marshalledBytes = null;

        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dout.writeInt(getMessageType());

        byte[] nodeKeyBytes = nodeKey.getBytes();
        int elementLength = nodeKeyBytes.length;
        dout.writeInt(elementLength);
        dout.write(nodeKeyBytes);

        dout.writeInt(sendTracker);
        dout.writeInt(receiveTracker);
        dout.writeInt(relayTracker);

        dout.writeLong(sendSummation);
        dout.writeLong(receiveSummation);

        dout.flush();

        marshalledBytes = baOutputStream.toByteArray();

        baOutputStream.close();
        dout.close();
        return marshalledBytes;
    }

    public void printEventInfo() {
        System.out.println("MessageType: " + getMessageType() + ", nodeKey: " + nodeKey + ", sendTracker: " + sendTracker + ", receiveTracker: " + receiveTracker + ", relayTracker: " + relayTracker + ", sendSummation: " + sendSummation + ", receiveSummation: " + receiveSummation);
    }

    public int getMessageType() {
        return EventType.TRAFFIC_SUMMARY;
    }

    public String getNodeKey() {
        return nodeKey;
    }

    public int getSendTracker() {
        return sendTracker;
    }

    public int getReceiveTracker() {
        return receiveTracker;
    }

    public int getRelayTracker() {
        return relayTracker;
    }

    public long getSendSummation() {
        return sendSummation;
    }

    public long getReceiveSummation() {
        return receiveSummation;
    }
}
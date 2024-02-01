package csx55.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.DataOutputStream;

public class TaskCompleteEvent implements Event {
    String nodeKey;

    public TaskCompleteEvent(String nodeKey) {
        this.nodeKey = nodeKey;
    }   

    public TaskCompleteEvent(byte[] marshalledBytes) throws IOException{
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        int nodeKeyLength = din.readInt();
        byte[] nodeKeyBytes = new byte[nodeKeyLength];
        din.readFully(nodeKeyBytes);

        this.nodeKey = new String(nodeKeyBytes);

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

        dout.flush();

        marshalledBytes = baOutputStream.toByteArray();

        baOutputStream.close();
        dout.close();
        return marshalledBytes;
    }

    public void printEventInfo() {
        System.out.println("MessageType: " + getMessageType() + ", additionalInfo: " + nodeKey);
    }

    public int getMessageType() {
        return EventType.TASK_COMPLETE;
    }

    public String getNodeKey() {
        return nodeKey;
    }
}
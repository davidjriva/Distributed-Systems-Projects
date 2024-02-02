package csx55.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.DataOutputStream;
import java.util.ArrayList;

public class MessagingNodesListEvent implements Event {
    /*
        Takes in a source that the message is going to be routed to 
    */
    int numConnections;
    ArrayList<String> hostNamePortList;

    public MessagingNodesListEvent(int numConnections, ArrayList<String> hostNamePortList) {
        this.numConnections = numConnections;
        this.hostNamePortList = hostNamePortList;
    }

    public MessagingNodesListEvent(byte[] marshalledBytes) throws IOException{
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        this.numConnections = din.readInt();

        this.hostNamePortList = new ArrayList<>();
        for (int i = 0; i < numConnections; i++) {
            int elementLength = din.readInt();
            byte[] hostNamePortBytes = new byte[elementLength];
            din.readFully(hostNamePortBytes);
            String hostNamePort = new String(hostNamePortBytes);
            hostNamePortList.add(hostNamePort);
        }

        baInputStream.close();
        din.close();
    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledBytes = null;

        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dout.writeInt(getMessageType());

        dout.writeInt(numConnections);

        for (String hostNamePort : hostNamePortList){
            byte[] hostNamePortBytes = hostNamePort.getBytes();
            int elementLength = hostNamePortBytes.length;
            dout.writeInt(elementLength);
            dout.write(hostNamePortBytes);
        }

        dout.flush();

        marshalledBytes = baOutputStream.toByteArray();

        baOutputStream.close();
        dout.close();
        return marshalledBytes;
    }

    public void printEventInfo() {
        System.out.println("MessageType: " + getMessageType() + ", numConnections: " + numConnections + ", hostNamePortList: " + hostNamePortList);
    }

    public int getMessageType() {
        return EventType.MESSAGING_NODES_LIST;
    }

    public int getNumConnections() {
        return numConnections;
    }

    public ArrayList<String> getHostNamePortList() {
        return hostNamePortList;
    }
}
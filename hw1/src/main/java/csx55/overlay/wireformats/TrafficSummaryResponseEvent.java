package csx55.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.DataOutputStream;

public class TrafficSummaryResponseEvent implements Event {
    byte statusCode; // 0 = success, 1 = failure

    public TrafficSummaryResponseEvent(byte statusCode) { 
        this.statusCode = statusCode;
    }

    public TrafficSummaryResponseEvent(byte[] marshalledBytes) throws IOException { 
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        this.statusCode = din.readByte();

        baInputStream.close();
        din.close();
    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledBytes = null;

        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dout.writeInt(getMessageType());

        dout.writeByte(statusCode);

        dout.flush();

        marshalledBytes = baOutputStream.toByteArray();

        baOutputStream.close();
        dout.close();
        return marshalledBytes;
    }  

    public void printEventInfo() {
        System.out.println("MessageType: " + getMessageType() + ", statusCode: " + statusCode);
    }

    public int getMessageType() {
        return EventType.TRAFFIC_SUMMARY_RESPONSE;
    }

    public byte getStatusCode() {
        return statusCode;
    }
}
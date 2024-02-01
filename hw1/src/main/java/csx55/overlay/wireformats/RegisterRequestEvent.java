package csx55.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.DataOutputStream;

public class RegisterRequestEvent implements Event {
    String ipAddress;
    int portNum;

    public RegisterRequestEvent(String ipAddress, int portNum) {
        this.ipAddress = ipAddress;
        this.portNum = portNum;
    }

    public RegisterRequestEvent(byte[] marshalledBytes) throws IOException{
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        // reads in ipAddress
        int ipAddressLength = din.readInt();
        byte[] ipAddressBytes = new byte[ipAddressLength];
        din.readFully(ipAddressBytes);

        this.ipAddress = new String(ipAddressBytes);

        //reads in portNum
        this.portNum = din.readInt();

        baInputStream.close();
        din.close();
    }

    // Allows writing a messageType, ipAddress, and portNum in that order
    public byte[] getBytes() throws IOException {
        byte[] marshalledBytes = null;

        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dout.writeInt(getMessageType());

        byte[] ipAddressBytes = ipAddress.getBytes();
        int elementLength = ipAddressBytes.length;
        dout.writeInt(elementLength);
        dout.write(ipAddressBytes);

        dout.writeInt(portNum);

        dout.flush();

        marshalledBytes = baOutputStream.toByteArray();

        baOutputStream.close();
        dout.close();
        return marshalledBytes;
    }

    public void printEventInfo() {
        System.out.println("MessageType: " + getMessageType() + ", ipAddress: " + ipAddress + ", port num: " + portNum);
    }

    public int getMessageType() {
        return EventType.REGISTER_REQUEST;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public int getPortNum() {
        return portNum;
    }
}
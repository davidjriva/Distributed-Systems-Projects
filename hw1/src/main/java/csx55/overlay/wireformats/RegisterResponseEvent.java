package csx55.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.DataOutputStream;

public class RegisterResponseEvent implements Event {
    byte statusCode;
    String additionalInfo;

    public RegisterResponseEvent(byte statusCode, String additionalInfo) {
        this.statusCode = statusCode;
        this.additionalInfo = additionalInfo;
    }

    public RegisterResponseEvent(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        this.statusCode = din.readByte();

        int additionalInfoLength = din.readInt();
        byte[] additionalInfoBytes = new byte[additionalInfoLength];
        din.readFully(additionalInfoBytes);

        this.additionalInfo = new String(additionalInfoBytes);

        baInputStream.close();
        din.close();
    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledBytes = null;

        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dout.writeInt(getMessageType());

        dout.writeByte(statusCode);

        byte[] additionalInfoBytes = additionalInfo.getBytes();
        int elementLength = additionalInfoBytes.length;
        dout.writeInt(elementLength);
        dout.write(additionalInfoBytes);

        dout.flush();

        marshalledBytes = baOutputStream.toByteArray();

        baOutputStream.close();
        dout.close();
        return marshalledBytes;
    }

    public void printEventInfo() {
        System.out.println("MessageType: " + getMessageType() + ", statusCode: " + statusCode + ", additionalInfo: " + additionalInfo);
    }

    public int getMessageType() {
        return EventType.REGISTER_RESPONSE;
    }

    public byte getStatusCode() {
        return statusCode;
    }

    public String getAdditionalInfo() {
        return additionalInfo;
    }
}
package csx55.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.DataOutputStream;

public class TaskInitiateEvent implements Event{
    int rounds;

    public TaskInitiateEvent(int rounds) {
        this.rounds = rounds;
    }

    public TaskInitiateEvent(byte[] marshalledBytes) throws IOException{
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        this.rounds = din.readInt();

        baInputStream.close();
        din.close();
    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledBytes = null;

        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dout.writeInt(getMessageType());

        dout.writeInt(rounds);

        dout.flush();

        marshalledBytes = baOutputStream.toByteArray();

        baOutputStream.close();
        dout.close();
        return marshalledBytes;
    }

    public void printEventInfo() {
        System.out.println("MessageType: " + getMessageType() + ", rounds: " + rounds);
    }

    public int getMessageType() {
        return EventType.TASK_INITIATE;
    }

    public int getRounds() {
        return rounds;
    }
}
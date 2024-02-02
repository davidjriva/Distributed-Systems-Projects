package csx55.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.DataOutputStream;
import java.util.ArrayList;

public class LinkWeightsEvent implements Event{
    int numLinks;
    ArrayList<String> links;

    public LinkWeightsEvent(int numLinks, ArrayList<String> links) {
        this.numLinks = numLinks;
        this.links = links;
    }

    public LinkWeightsEvent(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        this.numLinks = din.readInt();

        this.links = new ArrayList<>();
        for (int i = 0; i < numLinks; i++) {
            int elementLength = din.readInt();
            byte[] linkBytes = new byte[elementLength];
            din.readFully(linkBytes);
            String link = new String(linkBytes);
            links.add(link);
        }

        baInputStream.close();
        din.close();
    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledBytes = null;

        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dout.writeInt(getMessageType());

        dout.writeInt(numLinks);

        for (String link : links){
            byte[] linkBytes = link.getBytes();
            int elementLength = linkBytes.length;
            dout.writeInt(elementLength);
            dout.write(linkBytes);
        }

        dout.flush();

        marshalledBytes = baOutputStream.toByteArray();

        baOutputStream.close();
        dout.close();
        return marshalledBytes;
    }
    
    public void printEventInfo() {
        System.out.println("MessageType: " + getMessageType() + ", numLinks: " + numLinks + ", links: " + links);
    }

    public int getMessageType() {
        return EventType.LINK_WEIGHTS;
    }

    public int getNumLinks() {
        return numLinks;
    }

    public ArrayList<String> getLinks() {
        return links;
    }
}
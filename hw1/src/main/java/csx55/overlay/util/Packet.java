package csx55.overlay.util;

/*
    Wrapper class that holds:
        String destination -- a destination that the message should be sent to via TCPSenderThread
        byte[] -- The byte[] message that will be routed to the destination
*/
public class Packet {
    private final String key;
    private final byte[] message;

    public Packet(String key, byte[] message) {
        this.key = key;
        this.message = message;
    }

    public String getKey() {
        return key;
    }

    public byte[] getMessage() {
        return message;
    }
}
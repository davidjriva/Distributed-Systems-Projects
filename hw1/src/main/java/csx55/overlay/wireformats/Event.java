package csx55.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.DataOutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public interface Event {
    public byte[] getBytes() throws IOException;

    public int getMessageType();

    public void printEventInfo();
}
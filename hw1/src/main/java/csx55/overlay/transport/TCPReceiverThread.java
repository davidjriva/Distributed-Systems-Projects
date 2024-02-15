package csx55.overlay.transport;

import csx55.overlay.node.Node;
import csx55.overlay.node.Registry;
import csx55.overlay.wireformats.EventFactory;
import csx55.overlay.wireformats.Event;
import java.net.Socket;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.SocketException;
import java.util.Arrays;

public class TCPReceiverThread implements Runnable {
    public Socket socket;
    public DataInputStream din;
    public Node node;

    public TCPReceiverThread(Socket socket, Node node) throws IOException {
        this.socket = socket;
        this.din = new DataInputStream(socket.getInputStream());
        this.node = node;
    }
    
    @Override
    public void run() {
        int dataLength;

        while (this.socket != null) {
            try {
                dataLength = din.readInt();

                byte[] data = new byte[dataLength];
                din.readFully(data, 0, dataLength);

                Event e = EventFactory.createEvent(data, socket);

                node.onEvent(e, socket);
            } catch (SocketException se) {
                //System.err.println(se.getMessage());
                break;
            } catch (IOException ioe) {
                //System.err.println(ioe.getMessage());
                break;
            }
        }
    }
}
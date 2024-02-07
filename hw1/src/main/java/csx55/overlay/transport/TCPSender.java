package csx55.overlay.transport;

import java.net.Socket;
import java.io.DataOutputStream;
import java.io.IOException;

public class TCPSender {
    public Socket socket;
    public DataOutputStream dout;

    public TCPSender(Socket socket) throws IOException {
        this.socket = socket;
        dout = new DataOutputStream(socket.getOutputStream());
    }

    public void sendData(byte[] dataToSend) throws IOException {
        // System.out.println("[" + Thread.currentThread().getId() + "] sending data.");
        int dataLength = dataToSend.length;
        dout.writeInt(dataLength);
        dout.write(dataToSend, 0, dataLength);
        dout.flush();
    }

    public void closeSender() {
        try{
            socket.close();
        } catch (IOException ioe) {
            System.err.println("TCPSender.java: Issue closing socket " + ioe.getMessage());
        }
    }
}
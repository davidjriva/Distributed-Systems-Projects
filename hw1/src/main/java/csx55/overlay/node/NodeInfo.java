package csx55.overlay.node;

import java.net.Socket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.IOException;
import csx55.overlay.transport.TCPSender;

public class NodeInfo {
    private String hostName;
    private String ipAddress;
    private int portNum;
    private TCPSender sender;
    private Socket socket;

    public NodeInfo(String hostName, String ipAddress, int portNum) {
        try{
            this.hostName = hostName; 
            this.ipAddress = ipAddress;
            this.portNum = portNum;
            
            // connects to server of the hostName and port provided
            Socket socket = new Socket(hostName, portNum);
            this.sender = new TCPSender(socket);
            this.socket = socket;
        } catch(UnknownHostException e) {
            System.err.println(e.getMessage());
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }

    public NodeInfo(String hostName, String ipAddress, int portNum, Socket socket) {
        try{
            this.hostName = hostName; 
            this.ipAddress = ipAddress;
            this.portNum = portNum;
            
            // connects to server of the hostName and port provided
            this.sender = new TCPSender(socket);
            this.socket = socket;
        } catch(UnknownHostException e) {
            System.err.println(e.getMessage());
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }

    public void printNodeInfo() {
        System.out.println("hostname: " + hostName + ", ipAddress: " + ipAddress + ", portNum: " + portNum);
    }

    public String getHostName() {
        return hostName;
    }

    public TCPSender getSender() {
        return sender;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public int getPortNum() {
        return portNum;
    }

    public Socket getSocket() {
        return socket;
    }

    public String getKey() {
        return hostName + ":" + portNum;
    }
}
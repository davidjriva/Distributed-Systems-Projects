package csx55.overlay.wireformats;


public class EventType {
    public static final int REGISTER_REQUEST = 0;
    public static final int REGISTER_RESPONSE = 1;
    public static final int DEREGISTER_REQUEST = 2;
    public static final int DEREGISTER_RESPONSE = 3;
    public static final int CONNECT_WITH_NEIGHBOR = 4;
    public static final int MESSAGING_NODES_LIST = 5;
    public static final int LINK_WEIGHTS = 6;
    public static final int TASK_INITIATE = 7;
    public static final int TRANSMIT_PAYLOAD = 8;
    public static final int TASK_COMPLETE = 9;
    public static final int TRAFFIC_SUMMARY = 10;
    public static final int TRAFFIC_SUMMARY_RESPONSE = 11;


    public static final byte REGISTER_SUCCESS = 0;
    public static final byte REGISTER_FAILURE = 1;

    public static final byte DEREGISTER_SUCCESS = 0;
    public static final byte DEREGISTER_FAILURE = 1;
}
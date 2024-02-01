package csx55.overlay.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/* 
    Tracks statistics relating to packages sent, received, and relayed by a node.
    All the variables and access to variables in this class has to be thread-safe as 
    each receiver thread will modify them simultaneously through their onEvent.

    sendTracker: # of messages sent by this node
    receiveTracker: # of messages received by this node
    relayTracker: # of messages relayed by this node
    sendSummation: sum of all payload values sent by this node
    receiveSummation: sum of all payload values received by this node

    atomicInteger: incrementAndGet() --> increments the integer and gets the value after the increment

*/ 

public class StatTracker {
    AtomicInteger sendTracker;
    AtomicInteger receiveTracker;
    AtomicInteger relayTracker;

    AtomicLong sendSummation;
    AtomicLong receiveSummation;

    public StatTracker() {
        this.sendTracker = new AtomicInteger(0);
        this.receiveTracker = new AtomicInteger(0);
        this.relayTracker = new AtomicInteger(0);

        this.sendSummation = new AtomicLong(0);
        this.receiveSummation = new AtomicLong(0);
    }

    public void incrementSendTracker() {
        sendTracker.incrementAndGet();
    }

    public void incrementReceiveTracker() {
        receiveTracker.incrementAndGet();
    }

    public void incrementRelayTracker() {
        relayTracker.incrementAndGet();
    }

    public void addToSendSummation(long value) {
        sendSummation.addAndGet(value);
    }

    public void addToReceiveSummation(long value) {
        receiveSummation.addAndGet(value);
    }

    public int getSendTracker() {
        return sendTracker.get();
    }

    public int getReceiveTracker() {
        return receiveTracker.get();
    }

    public int getRelayTracker() {
        return relayTracker.get();
    }

    public long getSendSummation() {
        return sendSummation.get();
    }

    public long getReceiveSummation() {
        return receiveSummation.get();
    }

    public void resetAllCounters() {
        sendTracker.set(0);
        receiveTracker.set(0);
        relayTracker.set(0);

        sendSummation.set(0L);
        receiveSummation.set(0L);
    }

    @Override
    public String toString() {
        return "StatTracker{" +
                "sendTracker=" + sendTracker +
                ", receiveTracker=" + receiveTracker +
                ", relayTracker=" + relayTracker +
                ", sendSummation=" + sendSummation +
                ", receiveSummation=" + receiveSummation +
                '}';
    }
}

package csx55.overlay.node;

public class Link {
    private NodeInfo from;
    private NodeInfo to;
    private int weight;

    public Link(NodeInfo from, NodeInfo to, int weight) {
        this.from = from;
        this.to = to;
        this.weight = weight;
    }

    public NodeInfo getFrom() {
        return from;
    }

    public NodeInfo getTo() {
        return to;
    }

    public int getWeight() {
        return weight;
    }

    public String toString() {
        return from.getKey() + " " + to.getKey() + " " + weight;
    }
}
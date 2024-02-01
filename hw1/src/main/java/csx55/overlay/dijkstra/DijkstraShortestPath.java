package csx55.overlay.dijkstra;

import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Collections;

public class DijkstraShortestPath {
    private ConcurrentHashMap<String, ArrayList<Edge>> graph;

    public DijkstraShortestPath(ConcurrentHashMap<String, ArrayList<String>> links) {
        this.graph = buildGraph(links);
    }

    private ConcurrentHashMap<String, ArrayList<Edge>> buildGraph(ConcurrentHashMap<String, ArrayList<String>> links) {
        ConcurrentHashMap<String, ArrayList<Edge>> graph = new ConcurrentHashMap<>();

        for (String source : links.keySet()) {
            ArrayList<Edge> neighbors = new ArrayList<>();

            for (String destinationWeight : links.get(source)) {
                String[] parts = destinationWeight.split("--");

                String destination = parts[0];
                int weight = Integer.parseInt(parts[1]);

                neighbors.add(new Edge(destination, weight));
            }

            graph.put(source, neighbors);
        }

        return graph;
    }

    // Dijkstra's algorithm to find the shortest path
    public ShortestPathResult findShortestPath(String startNode, String endNode) {
        HashMap<String, Integer> distances = new HashMap<>();
        HashMap<String, String> previousNodes = new HashMap<>();
        PriorityQueue<NodeDistance> priorityQueue = new PriorityQueue<>();
        
        for (String node : graph.keySet()) {
            distances.put(node, Integer.MAX_VALUE);
            previousNodes.put(node, null);
        }
        distances.put(startNode, 0);
        priorityQueue.add(new NodeDistance(startNode, 0));

        while (!priorityQueue.isEmpty()) {
            NodeDistance current = priorityQueue.poll();

            for (Edge neighbor : graph.get(current.node)) {

                int newDistance = distances.get(current.node) + neighbor.weight;

                if (newDistance < distances.get(neighbor.destination)) {
                    distances.put(neighbor.destination, newDistance);
                    previousNodes.put(neighbor.destination, current.node);
                    priorityQueue.add(new NodeDistance(neighbor.destination, newDistance));
                }
            }
        }

        // Reconstruct the path
        ArrayList<String> shortestPath = new ArrayList<>();
        String currentNode = endNode;
        while (currentNode != null) {
            shortestPath.add(currentNode);
            currentNode = previousNodes.get(currentNode);
        }
        Collections.reverse(shortestPath);

        return new ShortestPathResult(distances.get(endNode), shortestPath);
    }

    private class NodeDistance implements Comparable<NodeDistance> {
        String node;
        int distance;

        NodeDistance(String node, int distance) {
            this.node = node;
            this.distance = distance;
        }

        @Override
        public int compareTo(NodeDistance other) {
            return Integer.compare(this.distance, other.distance);
        }
    }

    private class Edge {
        String destination;
        int weight;

        Edge(String destination, int weight) {
            this.destination = destination;
            this.weight = weight;
        }
    }
}
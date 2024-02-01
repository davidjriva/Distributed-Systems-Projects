package csx55.overlay.dijkstra;

import java.util.ArrayList;

public class ShortestPathResult {
    private int distance;
    private ArrayList<String> path;

    public ShortestPathResult(int distance, ArrayList<String> path) {
        this.distance = distance;
        this.path = path;
    }

    public int getDistance() {
        return distance;
    }

    public ArrayList<String> getPath() {
        return path;
    }
    
    // toString() for testing purposes
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("Distance: ").append(distance).append("\n");
        result.append("Path: ").append(pathToString()).append("\n");
        return result.toString();
    }

    private String pathToString() {
        if (path == null || path.isEmpty()) {
            return "No path";
        }

        StringBuilder pathString = new StringBuilder();
        for (int i = 0; i < path.size(); i++) {
            pathString.append(path.get(i));
            if (i < path.size() - 1) {
                pathString.append(" -> ");
            }
        }
        return pathString.toString();
    }
}
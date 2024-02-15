package csx55.threads;

public class ThreadUtils {
    public static void printWithThreadId(String message) {
        long threadId = Thread.currentThread().getId();
        System.out.println("Thread ID " + threadId + ": " + message);
    }
}
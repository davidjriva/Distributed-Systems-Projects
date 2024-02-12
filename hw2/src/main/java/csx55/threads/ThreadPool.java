package csx55.threads;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.List;
import java.util.ArrayList;

public class ThreadPool {
    private BlockingQueue<Runnable> taskQueue;
    private List<RunnableTask> tasks = new ArrayList<>();

    public ThreadPool(int numThreads, int capacity) {
        taskQueue = new ArrayBlockingQueue<>(capacity);

        // Setup threads 
        for (int i = 0; i < numThreads; i++) {
            RunnableTask RunnableTask = new RunnableTask(taskQueue);
            tasks.add(RunnableTask);
        }

        // Start all threads
        for (RunnableTask task : tasks) {
            new Thread(task).start();
        }
    }

    // Place an item into the queue, waits for space if queue is full
    public synchronized void addTask(Runnable task) {
        try{
            taskQueue.put(task);
        } catch (InterruptedException ie) {
            System.err.println("ThreadPool.java: " + ie.getMessage());
        }
    }

    public synchronized void stop() {
        for (RunnableTask task : tasks) {
            task.stop();
        }
    }
}
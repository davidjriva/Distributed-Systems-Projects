package csx55.threads;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.ArrayList;

public class ThreadPool {
    private ConcurrentLinkedQueue<Runnable> taskQueue;
    private List<RunnableTask> tasks = new ArrayList<>();
    private AtomicInteger queueSize;

    public ThreadPool(int numThreads, int capacity) {
        this.taskQueue = new ConcurrentLinkedQueue<>();
        this.queueSize = new AtomicInteger(0);

        // Setup threads 
        for (int i = 0; i < numThreads; i++) {
            RunnableTask RunnableTask = new RunnableTask(taskQueue, queueSize);
            tasks.add(RunnableTask);
        }

        // Start all threads
        for (RunnableTask task : tasks) {
            new Thread(task).start();
        }
    }

    // Place an item into the queue, waits for space if queue is full
    public void addTask(Runnable task) {
        taskQueue.add(task);
        queueSize.getAndIncrement();
    }

    public synchronized void stop() {
        for (RunnableTask task : tasks) {
            task.stop();
        }
    }
}
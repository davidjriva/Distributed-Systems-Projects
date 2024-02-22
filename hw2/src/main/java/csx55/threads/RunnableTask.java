package csx55.threads;

import java.util.concurrent.BlockingQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class RunnableTask implements Runnable{
    private Thread thread;
    private final ConcurrentLinkedQueue<Runnable> taskQueue;
    private boolean isRunning;
    private AtomicInteger queueSize;

    public RunnableTask(ConcurrentLinkedQueue<Runnable> taskQueue, AtomicInteger queueSize) {
        this.taskQueue = taskQueue;
        this.isRunning = true;
        this.queueSize = queueSize;
    }

    @Override
    public void run() {
        this.thread = Thread.currentThread();

        while (isRunning) {
            if(queueSize.get() > 0) {
                if(queueSize.decrementAndGet() >= 0) {
                    taskQueue.poll().run();
                } else {
                    queueSize.getAndIncrement();
                }
            }
        }
    }
    
    // Stops the thread. Will break it out of the taskQueue.take() blocking call if it's blocking.
    public void stop() {
        isRunning = false;
        this.thread.interrupt(); 
    }
}
package csx55.threads;

import java.util.concurrent.BlockingQueue;

public class RunnableTask implements Runnable{
    private Thread thread;
    private final BlockingQueue<Runnable> taskQueue;
    private boolean isRunning;

    public RunnableTask(BlockingQueue<Runnable> taskQueue) {
        this.taskQueue = taskQueue;
        this.isRunning = true;
    }

    @Override
    public void run() {
        this.thread = Thread.currentThread();

        while (isRunning) {
            try{
                Runnable runnable = (Runnable) taskQueue.take();
                runnable.run();
            } catch (InterruptedException ie) {
                System.err.println(ie.getMessage());
            }
        }
    }
    
    // Stops the thread. Will break it out of the taskQueue.take() blocking call if it's blocking.
    public void stop() {
        isRunning = false;
        this.thread.interrupt(); 
    }
}
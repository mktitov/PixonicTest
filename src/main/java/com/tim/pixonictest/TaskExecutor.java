/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tim.pixonictest;

import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author Mikhail Titov
 */
public class TaskExecutor {
    //для полной гарантии очередности выполнения задач необходимо выставить в единицу
    public final static int THREADS_COUNT = Runtime.getRuntime().availableProcessors(); 
    public final static long MAX_POLL_INTERVAL = 1000L;
    //что бы показать, что и после перехода в отрицательное множество значений все будет работать нормально
    private final static AtomicLong SEQUENCE = new AtomicLong(Long.MAX_VALUE-10); 
    
    private final ExecutorService tasksExecutor;
    private final ExecutorService scheduleTaskExecutor;
    private final DelayQueue<DelayedTask> delayedTasks;
    private final AtomicBoolean stopped;

    public TaskExecutor() {
        this(THREADS_COUNT);
    }
    
    public TaskExecutor(int threadsCount) {
        this.tasksExecutor = Executors.newWorkStealingPool(threadsCount);
        this.delayedTasks = new DelayQueue<>();
        this.stopped = new AtomicBoolean();
        this.scheduleTaskExecutor = Executors.newSingleThreadExecutor();        
        this.scheduleTaskExecutor.execute(new ScheduleWorker());
    }
    
    public <V> Future<V> execute(Date dateTime, Callable<V> task) {
        FutureTask<V> future = new FutureTask<>(task);
        delayedTasks.offer(new DelayedTask(dateTime.getTime(), future, task));
        return future;
    }
    
    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            scheduleTaskExecutor.shutdown();
            tasksExecutor.shutdown();            
        }
    }
    
    private class ScheduleWorker implements Runnable {
        
        @Override 
        public void run() {
            while (!stopped.get()) {
                try {
                    DelayedTask task = delayedTasks.poll(MAX_POLL_INTERVAL, TimeUnit.MILLISECONDS);
                    if (task!=null) 
                        tasksExecutor.execute(task.task);                        
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();                    
                }
            }
        }        
    }
    
    private class DelayedTask<V> implements Delayed {
        
        private final long startTime;
        private final FutureTask<V> task;
        private final Callable<V> originalTask;
        private final long seqNumber = SEQUENCE.getAndIncrement();

        public DelayedTask(long startTime, FutureTask<V> task, Callable<V> originalTask) {
            this.startTime = startTime;
            this.task = task;
            this.originalTask = originalTask;
        }
        
        @Override
        public long getDelay(TimeUnit tu) {
            return tu.convert(startTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed dt) {
            if (dt==this)
                return 0;
            DelayedTask t = (DelayedTask) dt;
            if (seqNumber==t.seqNumber)
                return 0;
            else
                return seqNumber - t.seqNumber < 0? -1 : 1;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj==this)
                return true;
            else if (!(obj instanceof DelayedTask))
                return false;
            else 
                return this.startTime==((DelayedTask)obj).startTime;
        }

        @Override
        public String toString() {
            return originalTask.toString(); 
        }
    }
}

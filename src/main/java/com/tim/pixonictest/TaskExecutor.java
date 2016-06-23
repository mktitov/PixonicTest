/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tim.pixonictest;

import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Mikhail Titov
 */
public class TaskExecutor {
    public final static int THREADS_COUNT = Runtime.getRuntime().availableProcessors(); //для полной гарантии очередности выполнения задач необходимо выставить в единицу :)
    
    private final ScheduledThreadPoolExecutor executor;

    public TaskExecutor() {
        this.executor = new ScheduledThreadPoolExecutor(THREADS_COUNT);
    }
    
    public TaskExecutor(int threadsCount) {
        this.executor = new ScheduledThreadPoolExecutor(THREADS_COUNT);        
    }

    public <V> ScheduledFuture<V> execute(Date dateTime, Callable<V> task) {
        return executor.schedule(task, dateTime.getTime() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }
}

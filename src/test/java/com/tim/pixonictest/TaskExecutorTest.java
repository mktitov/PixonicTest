package com.tim.pixonictest;

import java.util.Date;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mikhail Titov
 */
public class TaskExecutorTest {
    public final static int STRESS_TEST_TASKS_COUNT = 100_000;
    public final static int STRESS_TEST_EXECUTION_TIME = 10_000;
    
    private final Queue<Task> startedTasks = new ConcurrentLinkedQueue<>();
    
    @Test
    public void singleTaskTest() throws InterruptedException {
        TaskExecutor executor = new TaskExecutor(1);
        execute(executor, 1000);
        Thread.sleep(1100L);        
        checkQueue(1, 100);
    }
    
    @Test
    public void unsortedTasksTest() throws InterruptedException {
        TaskExecutor executor = new TaskExecutor();
        execute(executor, 1000);
        execute(executor, 500);
        Thread.sleep(1100L);        
        checkQueue(2, 100);        
    }
    
    @Test
    public void overdueTaskTest() throws InterruptedException {
        TaskExecutor executor = new TaskExecutor();
        execute(executor, -500);
        execute(executor, -1000);
        execute(executor, 500);
        Thread.sleep(1000L);        
        checkQueue(3, 100);        
    }
    
    @Test
    public void stressTest() throws InterruptedException {
        TaskExecutor executor = new TaskExecutor();
        Random rnd = new Random();
        
        for (int i=0; i<STRESS_TEST_TASKS_COUNT; i++) {
            execute(executor, rnd.nextInt(STRESS_TEST_EXECUTION_TIME));
        }
        Thread.sleep(STRESS_TEST_EXECUTION_TIME+1000L);
        checkQueue(STRESS_TEST_TASKS_COUNT, STRESS_TEST_EXECUTION_TIME);
    }
    
    private void checkQueue(int expectedSize, long maxDelay) {
        int size = 0;
        for (Task task: startedTasks) {
            size++;            
            assertFalse(task.execTime < task.planedExecTime.getTime()-1 || task.getExecutionTimeDelta() > maxDelay);
        }
        assertEquals(expectedSize, size);
    }
    
    private void execute(TaskExecutor executor, long delay) {
        Task task = new Task(System.currentTimeMillis()+delay);
        executor.execute(task.getPlanedExecTime(), task);        
    }
    
    private class Task implements Callable {
        private final Date planedExecTime;
        private final long submitTime;
        private volatile long execTime;

        public Task(long execTime) {            
            this.planedExecTime = new Date(execTime);
            this.submitTime = System.currentTimeMillis();
        }
        
        private boolean isOverdued() {
            return submitTime>planedExecTime.getTime();
        }
        
        public long getExecutionTimeDelta() {
            return isOverdued()? execTime - submitTime : execTime - planedExecTime.getTime();
        }

        public long getExecTime() {
            return execTime;
        }

        public Date getPlanedExecTime() {
            return planedExecTime;
        }
        
        @Override
        public Object call() throws Exception {
            execTime = System.currentTimeMillis();
            startedTasks.offer(this);
            return null;
        }        
    }
}

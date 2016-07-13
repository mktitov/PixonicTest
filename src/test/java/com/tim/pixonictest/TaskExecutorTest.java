package com.tim.pixonictest;

import java.util.Date;
import java.util.PriorityQueue;
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
        checkQueue(1, 100, false);
    }
    
    @Test
    public void orderedExecutionTest() throws Exception {
        long startTime = System.currentTimeMillis() + 2000;        
        TaskExecutor executor = new TaskExecutor(1);
        for (int i=0; i<100; i++)
            execute(executor, startTime, i);
        Thread.sleep(3000L);
        checkQueue(100, 100, true);
    }
    
    @Test
    public void unsortedTasksTest() throws InterruptedException {
        TaskExecutor executor = new TaskExecutor();
        execute(executor, 1000);
        execute(executor, 500);
        Thread.sleep(1100L);        
        checkQueue(2, 100, false);        
    }
    
    @Test
    public void overdueTaskTest() throws InterruptedException {
        TaskExecutor executor = new TaskExecutor();
        execute(executor, -500);
        execute(executor, -1000);
        execute(executor, 500);
        Thread.sleep(1000L);        
        checkQueue(3, 100, false);        
    }
    
    @Test
    public void stressTest() throws InterruptedException {
        TaskExecutor executor = new TaskExecutor();
        Random rnd = new Random();
        
        for (int i=0; i<STRESS_TEST_TASKS_COUNT; i++) {
            execute(executor, rnd.nextInt(STRESS_TEST_EXECUTION_TIME));
        }
        Thread.sleep(STRESS_TEST_EXECUTION_TIME+1000L);
        checkQueue(STRESS_TEST_TASKS_COUNT, STRESS_TEST_EXECUTION_TIME, false);
    }
    
    
    private void checkQueue(int expectedSize, long maxDelay, boolean checkOrder) {
        int size = 0;
        for (Task task: startedTasks) {
            assertFalse(task.execTime < task.planedExecTime.getTime()-1 || task.getExecutionTimeDelta() > maxDelay);
            if (checkOrder)
                assertEquals(size, task.orderNum);
            size++;            
        }
        assertEquals(expectedSize, size);
    }
    
    private void execute(TaskExecutor executor, long delay) {
        execute(executor, delay, 0);
    }
    
    private void execute(TaskExecutor executor, long startTime, int orderNum) {
        Task task = new Task(startTime, orderNum);
        executor.execute(task.getPlanedExecTime(), task);        
    }
    
    private class Task implements Callable {
        private final Date planedExecTime;
        private final long submitTime;
        private volatile long execTime;
        private final int orderNum;

        public Task(long execTime) {
            this(execTime, 0);
        }

        public Task(long execTime, int orderNum) {            
            this.planedExecTime = new Date(execTime);
            this.submitTime = System.currentTimeMillis();
            this.orderNum = orderNum;
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
//            System.out.println("Executig task");
            return null;
        }

        @Override
        public String toString() {
            return "Order num: "+orderNum; 
        }        
    }    
}

package org.example.experiment.coflow;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * @author alecHe
 * @desc ...
 * @date 2024-06-18 13:03:34
 */

public class Consumer implements Runnable {
    private static class AlloState{
        private int backlog;
        private int cnt;
        public AlloState(int backlog, int cnt){
            this.backlog = backlog;
            this.cnt = cnt;
        }

        public int getBacklog() {
            return backlog;
        }

        public void setBacklog(int backlog) {
            this.backlog = backlog;
        }

        public int getCnt() {
            return cnt;
        }

        public void setCnt(int cnt) {
            this.cnt = cnt;
        }
    }
    private volatile boolean isRunning = true;
    private BlockingQueue<Integer> queue; // 内存缓冲区
    private static final Object lock = new Object();
    private static int remainBuffer;
    private int count = 0;
//    private static final int SLEEPTIME = 500;
    private static Map<Long, AlloState> historyAllocation;
    private final int capacity;

    public Consumer(BlockingQueue<Integer> queue) {
        this.queue = queue;
        historyAllocation = new HashMap<>();
        capacity = 12;
        remainBuffer = 12;
    }

    @Override
    public void run() {
        int data;
        System.out.println("start consumer id = " + Thread.currentThread().getId());
        try {
            while (isRunning) { // 模拟延迟
                Thread.sleep(Constant.consumerDelay); // 从阻塞队列中获取数据
//                synchronized(lock){
//                    remainBuffer = capacity - queue.size();
//                }
                if (!queue.isEmpty()) {
                    data = queue.take();
//                    System.out.println("consumer " + Thread.currentThread().getId() + " consume data：" + data + ", size：" + queue.size());
                    count++;
                    synchronized(lock){
                        remainBuffer++;
                    }
                }
                else {
                    System.out.println("Queue is empty, consumer " + Thread.currentThread().getId() + " is waiting, size：" + queue.size());
                }
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
    public int getCount(){
        return count;
    }

    public static int LQF(long produceId, int requestData, int availableBuffer){
        if(!historyAllocation.containsKey(produceId)){
            historyAllocation.put(produceId, new AlloState(requestData, 1));
        }
        else{
            AlloState entry = historyAllocation.get(produceId);
            entry.backlog = entry.getBacklog() + requestData;
            entry.cnt++;
        }

        double sum = 0, cur = 0;
        for (Long key : historyAllocation.keySet()) {
            AlloState state = historyAllocation.get(key);
            sum += (double) state.backlog / state.cnt;
            if(key.equals(produceId)){
                cur =  (double) state.backlog / state.cnt;
            }
            // 处理键值对
        }
        System.out.println("aval size: " + availableBuffer +
          "\nproducer id:  " + produceId + " avg size: " +
          cur + " request size: " + requestData +
          " alloc size: " + min((int) (availableBuffer * (cur / sum) + 1),requestData));

        return min((int) (availableBuffer * (cur / sum) + 1),requestData);
    }

    public static int MSU(long produceId, int requestData, int availableBuffer){
        if(!historyAllocation.containsKey(produceId)){
            historyAllocation.put(produceId, new AlloState(requestData, 1));
        }
        else{
            AlloState entry = historyAllocation.get(produceId);
            entry.backlog = entry.getBacklog() + requestData;
            entry.cnt++;
        }

//        IloCplex cplex = new IloCplex();
        double sum = 0, cur = 0;
        for (Long key : historyAllocation.keySet()) {
            AlloState state = historyAllocation.get(key);
            sum += (double) state.backlog / state.cnt;
            if(key.equals(produceId)){
                cur =  (double) state.backlog / state.cnt;
            }
            // 处理键值对
        }
        return min((int) (availableBuffer * (cur / sum) + 1), requestData);
    }

    public static int Aminis(long produceId, int requestData, int availableBuffer, int dl){
        System.out.println("aval size: " + availableBuffer +
          "\nproducer id:  " + produceId + " request size: " + requestData +
          " alloc size: " + min(availableBuffer, dl));

        return dl;
    }

    public static int credit(long produceId, int requestData, int strategy, int dl){
        synchronized (lock){
            int res,tmp;
            switch (strategy){
                case 0:
                    return requestData;
                case 1:
                    res = min(requestData, remainBuffer);
                    System.out.println("aval size: " + remainBuffer +
                      "\nproducer id:  " + produceId + " request size: " + requestData +
                      " alloc size: " + res);
                    remainBuffer = max(0, remainBuffer - res);
                    return res;
                case 2:
                    res = LQF(produceId, requestData, remainBuffer);
                    remainBuffer = max(0, remainBuffer - res);
                    return res;
                case 3:
                    res = Aminis(produceId, requestData, remainBuffer, dl);
                    if(res <= remainBuffer){
                        remainBuffer -= res;
                        return res;
                    }
                    tmp = remainBuffer;
                    remainBuffer = 0;
                    return tmp;
                default:
                    res = MSU(produceId, requestData, remainBuffer);
                    remainBuffer = max(0, remainBuffer - res);
                    return res;
            }
        }
    }
    public void stop() { isRunning = false; }
}

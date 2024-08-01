package org.example.exp.coflow;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author alecHe
 * @desc ...
 * @date 2024-06-18 13:29:59
 */
public class DataSource implements Runnable {
    private volatile boolean isRunning = true;
    private BlockingQueue<Integer> queue; // 内存缓冲区
    private Integer sleepTime;
    private static AtomicInteger count = new AtomicInteger(); // 总数，原子操作
//    private static final long PENALTY = 50000;
    public DataSource(BlockingQueue<Integer> queue, Integer sleepTime) {
        this.queue = queue;
        this.sleepTime = sleepTime;
    }

    @Override
    public void run() {
        int data;

        double lambda = 1.0; // 平均每秒生成数据的次数，可以根据需要调整
        System.out.println("start data source id = " + Thread.currentThread().getId());
        while (isRunning) {
            try {
                // 模拟泊松分布的延迟
                int poissonDelay = generatePoissonRandom(lambda);
                // 构造任务数据
                data = count.incrementAndGet();
                // 通过匿名内部类创建并启动线程
                Thread.sleep((long) poissonDelay * sleepTime);
                // 往阻塞队列中添加数据
                if (!queue.offer(data, 10, TimeUnit.MILLISECONDS)) {
                    System.err.println("source " + Thread.currentThread().getId() + " failed to put data：" + data);
                    Thread.sleep(Constant.dataSourcePenalty);
                }
                else{
                    System.out.println("source " + Thread.currentThread().getId() + " send：" + data);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // 生成符合泊松分布的随机数
    private int generatePoissonRandom(double lambda) {
        Random random = new Random(1);
        // 三个数据集对应123456789L，123456788L，123456787L
        double L = Math.exp(-lambda);
        int k = 0;
        double p = 1;
        do {
            k++;
            double u = random.nextDouble();
            p *= u;
        } while (p > L);

        return k - 1;
    }
    public void stop() { isRunning = false; }
}

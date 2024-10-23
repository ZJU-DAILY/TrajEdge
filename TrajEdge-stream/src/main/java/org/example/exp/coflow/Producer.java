package org.example.exp.coflow;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// 115 LQF
// 115 FIFO
public class Producer implements Runnable {
    private volatile boolean isRunning = true;
    private BlockingQueue<Integer> queue; // 内存缓冲区
    private static AtomicInteger count = new AtomicInteger(); // 总数，原子操作
//    private static final long PENALTY = 10_000;
    private final int dl;
    private final int rt;
    private BlockingQueue<Integer> source;
    public static int strategy;

    public Producer(BlockingQueue<Integer> queue, BlockingQueue<Integer> source, int dl, int rt) {
        this.queue = queue; this.source =source;
        this.dl = dl;
        this.rt = rt;
    }

    @Override
    public void run() {
        int data = -1, remainData = 0;
        System.out.println("start producer id = " + Thread.currentThread().getId());
        try {
            while (isRunning) {
                int requestBuffer = source.size();
                int allowedBuffer = 0, i = 1;
                if(requestBuffer == 0)continue;
                // 这里得放一个delay啊，不然每次两层循环的意义何在呢？这个delay可以理解为定时的询问一下可用的credit
//                Thread.sleep(Constant.producerDelay + 500);

                allowedBuffer = Consumer.credit(Thread.currentThread().getId(), requestBuffer, strategy, this.dl);
                while (isRunning && !source.isEmpty() && i <= allowedBuffer) {
                    if(data == -1) data = source.take();
                    else data = remainData;
                    System.out.println("producer " + Thread.currentThread().getId() + " produce data：" + data + ", size：" + source.size());
                    // 往阻塞队列中添加数据
                    // 这个算作是消费者向生产者传输过程中需要的时间，主要是让source中的数据满一点
                    Thread.sleep(Constant.producerDelay * this.rt);
                    if (!queue.offer(data, 2, TimeUnit.SECONDS)) {
                        System.err.println("producer " + Thread.currentThread().getId() + " failed to put data：" + data);
                        Thread.sleep(Constant.producerPenalty);
                        remainData = data;
                    }
                    else {
                        i++;
                        data = -1;
                    }
                }
//                    else {
//                        System.out.println("Queue is empty, producer " + Thread.currentThread().getId() + " is waiting, size：" + source.size());
//                    }

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
    public void stop() { isRunning = false; }

    public static void main(String args[]) throws InterruptedException {

        strategy = Integer.parseInt(args[0]);
        System.out.println(strategy);
        // 1.构建内存缓冲区
        BlockingQueue<Integer> queue = new LinkedBlockingDeque<>(20);
        BlockingQueue<Integer> s1 = new LinkedBlockingDeque<>(5);
        BlockingQueue<Integer> s2 = new LinkedBlockingDeque<>(5);
        BlockingQueue<Integer> s3 = new LinkedBlockingDeque<>(5);

        int dl1 = 10, dl2 = 100, dl3 = 1000;

        // 2.建立线程池和线程
        ExecutorService service = Executors.newCachedThreadPool();
        DataSource source1 = new DataSource(s1, dl1);
        DataSource source2 = new DataSource(s2, dl2);
        DataSource source3 = new DataSource(s3, dl3);
        Producer prodThread1 = new Producer(queue,s1,10, 5);
        Producer prodThread2 = new Producer(queue,s2,10,1);
        Producer prodThread3 = new Producer(queue,s3,100,8);
        Consumer consThread1 = new Consumer(queue);

        service.execute(source1);
        service.execute(source2);
        service.execute(source3);
        service.execute(prodThread1);
        service.execute(prodThread2);
        service.execute(prodThread3);
        service.execute(consThread1);

        // 3.睡一会儿然后尝试停止生产者
         Thread.sleep(5 * 60 * 1000);
         source1.stop();
         source2.stop();
         source3.stop();
         prodThread1.stop();
         prodThread2.stop();
         prodThread3.stop();
         consThread1.stop();

         System.out.println(consThread1.getCount());
         // 4.再睡一会儿关闭线程池
         Thread.sleep(3000);
         service.shutdown();
    }
}







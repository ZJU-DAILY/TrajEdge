package org.example.dht;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.example.trajstore.TrajPoint;
import org.example.trajstore.TrajStore;
import org.example.trajstore.TrajStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author alecHe
 * @desc ...
 * @date 2024-01-27 16:51:27
 */

public class DataProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DataProcessor.class);
    private BlockingQueue<TrajPoint> dataQueue = new LinkedBlockingQueue<>();
    private TrajStore kvStore = null;

    public DataProcessor(TrajStore kvStore) {
        this.kvStore = kvStore;
    }

    public void addToQueue(TrajPoint point) {
        try {
            dataQueue.put(point);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void processData() {
        new Thread(() -> {
            while (true) {
                try {
                    TrajPoint data = dataQueue.take(); // 如果队列为空，线程会阻塞等待
                    writeToDisk(data);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break; // 线程被中断时退出循环
                }
            }
        }).start();
    }

    private void writeToDisk(TrajPoint point) throws InterruptedException {
        try {
            synchronized (kvStore){
                kvStore.insert(point);
            }
            LOG.info(point.toString());
        } catch (TrajStoreException e) {
            e.printStackTrace();
//            return false;
        }
    }

    public static void main(String[] args) {
//        DataProcessor processor = new DataProcessor();
//        processor.processData(); // 启动数据处理线程
//
//        // 模拟向队列中添加数据
//        for (int i = 0; i < 10; i++) {
//            processor.addToQueue("Data-" + i);
//            System.out.println("Add data : " + i);
//            try {
//                Thread.sleep(10); // 模拟数据产生的时间间隔
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
    }
}

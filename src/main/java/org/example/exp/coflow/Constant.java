package org.example.exp.coflow;// Aminis的思路是有一个带宽资源，然后在初始化的时候就进行分配。并且随着运行而发生变化
// 整体的思路是优先满足短作业，从而最大化runtime任务的完成数量

//消费者处理一个tuple需要500ms，生产者生成一个tuple需要10，100，1000 + （100）的时间
// 分母乘10000，可以得到消费者有20个资源，生产者1、2、3分别的需求大约是90，50，9.09
public class Constant {
  public static int consumerDelay = 450;
  public static int producerDelay = 100;
  public static int producerPenalty = 10_000;
  public static int dataSourcePenalty = 50000;

}

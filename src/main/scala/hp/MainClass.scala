package hp

/**
  * 程序入口
  * Created by yinmuyang on 16/9/9.
  */
object MainClass {
    def main(args: Array[String]) {
        System.err.println("Usage: MainClass <action>:0:预测文本分类1:构建模型" +
            " <brokers> <topics> <timeWindow> <numRepartition> <autooffset> <groupId> <pathPre:hdfs pre >")

        if (args.length <=0 ) {
            System.err.println("Usage: Common <brokers> <topics> <timeWindow> <numRepartition> <autooffset> <groupId> <pathPre:hdfs pre >")
            System.exit(1)
        }

        // 预测文本分类
        if(args(0)==0){

        }else if(args(0) == 1){// 构建随机森林模型

        }


    }
}

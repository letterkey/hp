package hp

import java.io.{ObjectInputStream, ObjectOutputStream}

import classficate.callct.utils.SplitwdUtil
import org.ansj.library.UserDefineLibrary
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{HashingTF, StringIndexer, Tokenizer}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.nlpcn.commons.lang.tire.domain.Forest

import scala.collection.mutable

/**
  * 构建训练模型
  * Created by yinmuyang on 16/9/5.
  */
object SaveModelRandomForest {

    def main(args: Array[String]) {
        var numClasses = 218

        val categoricalFeaturesInfo = Map[Int, Int]()

        var numTrees = 200

        var featureSubsetStrategy = "auto"

        var impurity = "gini"

        var maxDepth = 30

        var maxBins = 70

        var NumFeatures = 6900

        var seed = 1234

        var minInstancesPerNode: Int = 1

        var minInfoGain: Double = 0.0

        var cacheNodeIds: Boolean = false

        var checkpointInterval: Int = 10


        SplitwdUtil.csv = "./data/hp/TrainingDataSet.csv"

        SplitwdUtil.StopWord = "./data/hp/StopWordsU.txt"
        SplitwdUtil.mod = 10
        SplitwdUtil.NosplitWord = "./data/hp/NoSplitWords.txt"
        UserDefineLibrary.ambiguityForest=new Forest()

        SplitwdUtil.process

        val sparkConf = new SparkConf().setMaster("local").setAppName("RDDRelation")
        sparkConf.set("lableIndexPath","/hp/kv")
        val sc = new SparkContext(sparkConf)
        sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://192.168.100.73:9000")
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._

        val trainRaw = sc.parallelize(SplitwdUtil.train)
        trainRaw.toDF().show()
        val testRaw = sc.parallelize(SplitwdUtil.test)
        testRaw.toDF().show()
        val stages = new mutable.ArrayBuffer[PipelineStage]()

        // Tokenizer to process text fields
        val tokenizer = new Tokenizer()
            .setInputCol("text")
            .setOutputCol("words")
        stages += tokenizer

        // HashingTF to convert tokens to the feature vector
        val hashingTF = new HashingTF()
            .setInputCol("words")
            .setOutputCol("features")
            .setNumFeatures(NumFeatures)
        stages += hashingTF

        // Indexer to convert String labels to Double
        val indexer = new StringIndexer()
            .setInputCol("group")
            .setOutputCol("label")
            .fit(trainRaw.toDF)
        // 持久化(下标,标签)到hdfs
        val label_index = indexer.transform(trainRaw.toDF()).groupBy("label","group").count().collect().map(r =>(r.getDouble(0),r.getString(1))).toMap[Double,String]
        saveLabelIndex(label_index,sc)

        stages += indexer
        stages += new RandomForestClassifier()
            .setFeaturesCol("features")
            .setLabelCol("label")
            .setMaxDepth(maxDepth)
            .setMaxBins(maxBins)
            .setMinInstancesPerNode(minInstancesPerNode)
            .setMinInfoGain(minInfoGain)
            .setCacheNodeIds(cacheNodeIds)
            .setCheckpointInterval(checkpointInterval)
            .setFeatureSubsetStrategy(featureSubsetStrategy)
            .setNumTrees(numTrees)
            .setSeed(1234)

        val pipeline = new Pipeline().setStages(stages.toArray)
        // 以流的方式来处理源训练数据,并返回PipelineModel 实例
        val pipelineModel = pipeline.fit(trainRaw.toDF)
        pipelineModel.write.overwrite().save("./model")

        //预测评估模型
        testRaw.toDF.limit(1).show(false)
        // 执行分类
        val fullPredictionsT = pipelineModel.transform(testRaw.toDF).cache()

        val predictions = fullPredictionsT.select("prediction").map(_.getDouble(0))

        val labelsT = fullPredictionsT.select("label").map(_.getDouble(0))
        val zip = predictions.toJavaRDD.zip(labelsT.toJavaRDD)
        // 预测准确率
        val accuracy = new MulticlassMetrics(zip)
//        val accuracy = new MulticlassMetrics(predictions.zip(labelsT)).precision
        println("模型评估:",accuracy.precision.toDouble)
        println("模型评估:",accuracy.accuracy.toDouble)
    }

    /**
      * 保存标签和序号对象到 fs
      * @param kv
      * @param sc
      */
    def saveLabelIndex(kv : Map[Double,String],sc : SparkContext): Unit ={
        val fileSystem = FileSystem.get(sc.hadoopConfiguration)
        val path = new Path(sc.getConf.get("lableIndexPath"))
        val oos = new ObjectOutputStream(new FSDataOutputStream(fileSystem.create(path)))
        oos.writeObject(kv)
        oos.close
    }

    /**
      * 反序列化标签下表kv对象
      * @param sc
      * @return
      */
    def readLabelIndex(sc : SparkContext): Map[Double,String] ={
        val fileSystem = FileSystem.get(sc.hadoopConfiguration)
        val path = new Path(sc.getConf.get("lableIndexPath"))
        val ois = new ObjectInputStream(new FSDataInputStream(fileSystem.open(path)))
        val kv = ois.readObject.asInstanceOf[Map[Double,String]]
        kv
    }
}

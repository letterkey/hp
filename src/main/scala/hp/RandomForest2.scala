package hp

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.util.Properties

import classficate.callct.utils.SplitwdUtil
import org.ansj.library.UserDefineLibrary
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{HashingTF, StringIndexer, Tokenizer}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.nlpcn.commons.lang.tire.domain.Forest

import scala.collection.mutable

/**
  * 预测数据分类
  * Created by yinmuyang on 16/9/5.
  */
object RandomForest2 {
    val url = "jdbc:mysql://rdsul45bw6l06j60xe6z.mysql.rds.aliyuncs.com/test2?useUnicode=true&characterEncoding=UTF-8"
    val table = "hp_test_prediction"
    val properties = new Properties
    properties.put("user","sinosig")
    properties.put("password","sinosig")
    // model存放hdfs路径
    var hdfs = ""
    var modelPath="hdfs://123.57.173.154:9009/hp/model" // 随机森林model存放路径:测试环境为本地磁盘;
    var jobId=System.currentTimeMillis()
    var flag:String = "0"
    var forestArgs = "xxx 10"
    def main(args: Array[String]) {

//        val Array(flag,modelPath,forestArgs) = args
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDDRelation")
        val sc = new SparkContext(sparkConf)
        val forestArrgsArr = forestArgs.split(" ")
//        sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://192.168.100.73:9000")
        // 暂时将停止词和不需要分词的文件打包到jar中
        SplitwdUtil.csv = "./data/hp/TrainingDataSet.csv"
        SplitwdUtil.StopWord = "./data/hp/StopWordsU.txt"

        SplitwdUtil.mod =  forestArrgsArr.last.toInt
//        flag match {
//            case "0" => SplitwdUtil.mod = 10
//            case "1" => SplitwdUtil.mod = 1000
//        }
        SplitwdUtil.NosplitWord = "./data/hp/NoSplitWords.txt"
        UserDefineLibrary.ambiguityForest=new Forest()
        SplitwdUtil.process

        if(flag == "0"){// 构建模型
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

            val a = Array("218","200","auto","gini","30","70","6900","1234","1","0.0","false","10")
            makeModel(sc,a)
        }else if(flag == "1"){// 文本分类
            prediction(sc,args)
        }
        sc.stop()
    }

    /**
      * 创建模型
      * @param sc
      * @param args
      */
    def makeModel(sc:SparkContext,args: Array[String]): Unit ={
        val Array(
        numClasses,
        numTrees,
        featureSubsetStrategy,
        impurity,
        maxDepth,
        maxBins,
        numFeatures,
        seed,
        minInstancesPerNode,
        minInfoGain,
        cacheNodeIds,
        checkpointInterval) = args

        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._

        val trainRaw = sc.parallelize(SplitwdUtil.train)
        trainRaw.toDF().show()
        val testRaw = sc.parallelize(SplitwdUtil.test)
        testRaw.toDF().show(false)
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
            .setNumFeatures(numFeatures.toInt)
        stages += hashingTF

        // Indexer to convert String labels to Double
        val indexer = new StringIndexer()
            .setInputCol("group")
            .setOutputCol("label")
            .fit(trainRaw.toDF)
        // 持久化(下标,标签)到hdfs
        val label_index = indexer.transform(trainRaw.toDF()).groupBy("label","group").count()
        val saveLabelIndexToDFS = label_index.collect().map(r =>(r.getDouble(0),r.getString(1))).toMap[Double,String]

        // 持久化下标标签到mysql
        label_index.map(r => (r.getDouble(0),r.getString(1),jobId)).toDF("label","group","jobId")
            .write.mode(SaveMode.Append).jdbc(url,"hp_label_index",properties)
//        label_index.write.mode(SaveMode.Overwrite).jdbc(url,"hp_label_index",properties)

        stages += indexer
        stages += new RandomForestClassifier()
            .setFeaturesCol("features")
            .setLabelCol("label")
            .setMaxDepth(maxDepth.toInt)
            .setMaxBins(maxBins.toInt)
            .setMinInstancesPerNode(minInstancesPerNode.toInt)
            .setMinInfoGain(minInfoGain.toDouble)
            .setCacheNodeIds(cacheNodeIds.toBoolean)
            .setCheckpointInterval(checkpointInterval.toInt)
            .setFeatureSubsetStrategy(featureSubsetStrategy)
            .setNumTrees(numTrees.toInt)
            .setSeed(seed.toInt)

        val pipeline = new Pipeline().setStages(stages.toArray)
        // 以流的方式来处理源训练数据,并返回PipelineModel 实例
        val pipelineModel = pipeline.fit(trainRaw.toDF)
        pipelineModel.write.overwrite().save(modelPath)

        //预测评估模型
        testRaw.toDF.limit(1).show(false)
        // 执行分类
        val fullPredictionsT = pipelineModel.transform(testRaw.toDF).cache()
        fullPredictionsT.printSchema()
        val save = fullPredictionsT
            .select("group","text","orgtext","label","prediction")
            .map(r => (r.getString(0),r.getString(1),r.getString(2),r.getDouble(3),r.getDouble(4),jobId))

        save.toDF("group","text","orgtext","label","prediction","jobId").write.mode(SaveMode.Append).jdbc(url,"hp_test_prediction",properties)
        val predictions = fullPredictionsT.select("prediction").map(_.getDouble(0))

        val labelsT = fullPredictionsT.select("label").map(_.getDouble(0))
        val zip = predictions.toJavaRDD.zip(labelsT.toJavaRDD)
        // 预测准确率
        val accuracy = new MulticlassMetrics(zip)
        val saveAccuracy = sc.parallelize(Seq(accuracy.precision)).map((_,jobId)).toDF("modelAccuracy","jobId")
        saveAccuracy.show()
        saveAccuracy.write.mode(SaveMode.Append).jdbc(url,"hp_model_prediction",properties)
        //        val accuracy = new MulticlassMetrics(predictions.zip(labelsT)).precision
        println("模型评估:",accuracy.accuracy.toDouble)
    }

    /**
      * 预测分类
      * @param sc
      * @param args
      */
    def prediction(sc:SparkContext,args:Array[String]): Unit ={
        val Array(filePath,jobId,mod) = args

        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._

        val testRaw = sc.parallelize(SplitwdUtil.train)
        // 以流的方式来处理源训练数据,并返回PipelineModel 实例
        val pipelineModel = PipelineModel.load(modelPath)

        println("------------------------------------")
        testRaw.toDF.limit(1).show(false)
        val fullPredictionsT = pipelineModel.transform(testRaw.toDF).cache()
        fullPredictionsT.show()
        val save = fullPredictionsT.select("text","orgtext","features","label","rawPrediction","probability","prediction")
        save.map(r => (jobId,r.getString(0),r.getString(1),r.getDouble(3),r.getDouble(6))).toDF("jobId","text","orgtext","label","prediction")
            .write.mode(SaveMode.Append).jdbc(url,"hp_prediction",properties)
        println("------------------------------------")
        val predictions = fullPredictionsT.select("prediction").map(_.getDouble(0))

        predictions.collect().foreach(println)
//        val kv = SaveModelRandomForest.readLabelIndex(sc)
//        val labelsT = fullPredictionsT.select("group","label")
//            .map(r =>(r.getString(0),r.getDouble(1),kv.get(r.getDouble(1))))
//        labelsT.collect().foreach(println)
    }


    /**
      * 保存标签和序号对象到 fs
      * @param kv
      * @param sc
      */
    @DeveloperApi
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
    @DeveloperApi
    def readLabelIndex(sc : SparkContext): Map[Double,String] ={
        val fileSystem = FileSystem.get(sc.hadoopConfiguration)
        val path = new Path(sc.getConf.get("lableIndexPath"))
        val ois = new ObjectInputStream(new FSDataInputStream(fileSystem.open(path)))
        val kv = ois.readObject.asInstanceOf[Map[Double,String]]
        kv
    }
}

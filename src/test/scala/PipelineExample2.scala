/**
  * Created by yinmuyang on 16/9/7.
  */
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, IndexToString, StringIndexer, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.Row
// $example off$
import org.apache.spark.sql.SparkSession

object PipelineExample2 {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .master("local")
            .appName("PipelineExample")
            .getOrCreate()

        val df = spark.createDataFrame(Seq(
            (0, "a"),
            (1, "b"),
            (2, "c"),
            (3, "a"),
            (4, "a"),
            (5, "c")
        )).toDF("id", "category")

        val indexer = new StringIndexer()
            .setInputCol("category")
            .setOutputCol("categoryIndex")
            .fit(df)
        val indexed = indexer.transform(df)
        indexed.show()
        val x = indexed.groupBy("categoryIndex","category").count().orderBy("categoryIndex").collect().map(r => (r.getDouble(0),r.getString(1))).toMap[Double,String]

        println("--------",x.get(2.0))
        val converter = new IndexToString()
            .setInputCol("categoryIndex")
            .setOutputCol("originalCategory")

        val converted = converter.transform(indexed)
        val c = converted.collect().map(r => (r.getInt(0),r.getString(1))).toMap[Int,String]
        println("--------",c.get(2))

        converted.select("id", "originalCategory").show()
        spark.stop()
    }
}

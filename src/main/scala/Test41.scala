import org.apache.commons.codec.StringDecoder
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, SparkContext}
//import net.liftweb.json._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._




case class Person(name: String, age: Int, car: String) extends Serializable{
  
}


/*object SparkStreaming {
   def main(args: Array[String]): Unit = {

//
      val conf = new SparkConf().set("spark.mongodb.write.connection.uri", "mongodb://localhost:27017")
                                .set("spark.mongodb.write.database","sample_analytics")
                                .set("spark.mongodb.write.collection","dump2")
                                .set("spark.mongodb.read.connection.uri", "mongodb://localhost:27017")
                                .set("spark.mongodb.read.database","sample_analytics")
                                .set("spark.mongodb.read.collection","dump2")
                                .setMaster("spark://localhost:7077")
                                .setMaster("local[1]")
                                .setAppName("KafkaReceiver")

      val ssc = new StreamingContext(conf, Seconds(5));

      ssc.sparkContext.setLogLevel("ERROR")    
      val topics="transactions";
      val topicsSet = topics.split(",").toSet
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "test",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
      val messages = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
        
        val customerSchema = new StructType()
         .add("username",StringType)
         .add("name", StringType)
         .add("address", StringType)
         .add("birthdate", StringType)
         .add("email", StringType)
         .add("system_time", StringType)
         .add("accounts", ArrayType(IntegerType))
        
        
    // Get the lines, split them into words, count the words and print
    val words = messages.map(_.value)
//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
//    val data = lines.map(record => {
//      implicit val formats = DefaultFormats;
//      val jValue = parse(record);
//      val p:Person=jValue.extract[Person];
//      print(p.name)
//      p
//      //val df = spark.read.json(record.value.toString())
//
//    })
    //words.print();
    //val jValue = parse(lines.toString());
     
    
     words.foreachRDD { rdd =>

            // Get the singleton instance of SparkSession
            val spark = SparkSession.builder.config(rdd.sparkContext.getConf)
//                .config("spark.mongodb.output.uri", uri)
                .getOrCreate()
            import spark.implicits._
          
            
            val df = spark.read.format("mongodb").load()
            val readDF=df.select(col("name"),col("count"))

            // Convert RDD[String] to DataFrame
            val wordsDataFrame = rdd.toDF("word")
          
            // Create a temporary view
          //  wordsDataFrame.createOrReplaceTempView("words")
          //
          //  // Do word count on DataFrame using SQL and print it
          //  val wordCountsDataFrame = 
          //    spark.sql("select word, count(*) as total from words group by word")
          //  wordCountsDataFrame.show()
            
            //wordsDataFrame.printSchema()
            val structuredDF = wordsDataFrame.select(from_json(col("word"), customerSchema).as("data")).select("data.*")
            
            //structuredDF.show()
            
            //structuredDF.show();
            val aggDF = structuredDF.groupBy("name").count()
//            val resDF=readDF.union(readDF).dropDuplicates();
//            aggDF.show();
            //aggDF.show()
//            aggDF.write.format("mongodb").mode("append").save();
//             aggDF.writeStream
//            .format("mongodb")
//            .option("checkpointLocation", "/tmp/mongod/")
//            .option("forceDeleteTempCheckpointLocation", "true")
//            .option("spark.mongodb.connection.uri", uri)
//            .option("spark.mongodb.database", "transactions")
//            .option("spark.mongodb.collection", "dump2")
//            .outputMode("complete")
//            .start()
//            .awaitTermination()
     }
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
   }
}*/

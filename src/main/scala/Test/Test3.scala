package Test

import org.apache.spark.sql._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.log4j._
//import org.mongodb.scala._

import sparkFuncReqPackage._

object Test3 {
   val logger = Logger.getRootLogger
   logger.setLevel(Level.INFO)
   def main(args: Array[String]) {

        // initialize Spark
         val spark = SparkSession
            .builder
            .appName("Stream Handler")
            .config("spark.master", "local[*]")
            //.master("spark://ubuntu-desktop:7077")
            .getOrCreate()

        import spark.implicits._
        spark.sparkContext.setLogLevel("ERROR") 
        // read from Kafka
        val inputDF1 = spark
            .readStream
            .format("kafka") // org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("failOnDataLoss", "false")
            .option("subscribe", "transactions")
            .load()
            
        val inputDF2 = spark
            .readStream
            .format("kafka") // org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "customers")
            .load()

        val IdType = new StructType()
  		  .add("$oid", StringType)
  			 
    		val DateType = new StructType()
    		  .add("$date", TimestampType)
    		  
    		  
    		val transactionSchema = new StructType()
           .add("username",StringType)
           .add("name", StringType)
           .add("address", StringType)
           .add("location", StringType)
           .add("birthdate", StringType)
           .add("email", StringType)
           .add("system_time", StringType)
           .add("account_id", IntegerType)
           .add("transaction_count", IntegerType)
           //.add("transactions", ArrayType(innerSchema))
           .add("date", StringType)
      		 .add("amount", IntegerType)
      		 .add("transaction_code", StringType)
      		 .add("symbol", StringType)
      		 .add("price", StringType)
      		 .add("total", StringType)
//        val transactionSchema= new StructType()
//               .add("username",StringType)
//               .add("name", StringType)
//               .add("address", StringType)
//               .add("birthdate", StringType)
//               .add("email", StringType)
//               .add("system_time", StringType)
//               .add("account_id", IntegerType)
//               .add("date", StringType)
//               .add("amount", IntegerType)
//               .add("transaction_code", StringType)
//               .add("symbol", StringType)
//               .add("price", StringType)
//               .add("total", StringType)
//               .add("prev_date", StringType)
//               .add("prev_time", StringType) 
      	
      	val customerSchema = new StructType()
         .add("username",StringType)
         .add("name", StringType)
         .add("address", StringType)
         .add("birthdate", StringType)
         .add("email", StringType)
         .add("system_time", StringType)
         .add("accounts", ArrayType(IntegerType))
         
        val accountSchema = new StructType()
         .add("account_id",StringType)
         .add("limit", IntegerType)
         .add("products", ArrayType(StringType))
         .add("system_time", StringType)
       
          
        val rawDF1 = inputDF1.selectExpr("CAST(value AS STRING)").as[String]
        val rawDF2 = inputDF2.selectExpr("CAST(value AS STRING)").as[String]
        

        val structuredDF1 = rawDF1.select(from_json(col("value"), transactionSchema).as("data")).select("data.*")
        structuredDF1.printSchema()
        
        val structuredDF2 = rawDF2.select(from_json(col("value"), customerSchema).as("data")).select("data.*")
//        structuredDF2.printSchema()
        
//        val structuredDF3 = rawDF.select(from_json(col("value"), accountSchema).as("data")).select("data.*")
//        structuredDF3.printSchema()
      
    	
        val uri = "mongodb://localhost:27017/"
       
	    
         try{
           
           

//           val sDF1=structuredDF1.select(col("system_time")).withColumn("topic", lit("transaction"));
//           val sDF2=structuredDF2.select(col("system_time")).withColumn("topic", lit("customer"));
//           val currDF=sDF1.union(sDF2);
//           val resDF=currDF.groupBy("topic").count()
//           
//           val df3=structuredDF2.withColumn("age", year(col("birthdate")))
//                                .withColumn("acc_len", size(col("accounts")))
//                                
//           val temp_df = structuredDF1.withColumn("prev_timestamp", concat(col("prev_date"),lit("T"),col("prev_time")))
//                                      .withColumn("standard_time", to_timestamp(lit("2015-01-01T00:00:00")));
//    
//           val stockTradeDF = temp_df.filter(to_timestamp(col("date")) >= col("standard_time") && col("prev_timestamp") < col("standard_time") )
//                                     .groupBy("address")
//                                     .count() 
//                                
//            logger.info("first log")
           
           val resDF1=TransactionAmount.compute(structuredDF1)
           val resDF2=TransactionPattern.compute(structuredDF1)
           
           
           //df5.printSchema();
           structuredDF2.writeStream
            .format("mongodb")
            .option("checkpointLocation", "/tmp/mongod/rishi8")
            .option("forceDeleteTempCheckpointLocation", true)
            .option("spark.mongodb.connection.uri", "mongodb://localhost:27017/")
            .option("spark.mongodb.database", "sample_analytics")
            .option("spark.mongodb.collection", "dump2")
            .outputMode("append")
            .start()
            .awaitTermination()
          
//            resDF2.writeStream
//            .format("console")
//            .outputMode("complete")
//            .start()
//            .awaitTermination() 
            
         }catch{
           case a:Exception=>{
             println(a)
             System.exit(1)
           }
         }
         
    }
    
    
    
    def func(df : DataFrame) : DataFrame =
    {
       
       
       df.groupBy("location")
       .count()
       .orderBy("location")
       
    }
}
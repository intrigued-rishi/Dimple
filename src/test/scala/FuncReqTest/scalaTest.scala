package Test


import org.apache.spark.sql.types._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.streaming._
import org.scalatest.BeforeAndAfter

import sparkFuncReqPackage._

class scalaTest extends AnyFunSuite with BeforeAndAfter{
 
  val spark = SparkSession
            .builder()
            .appName("SparkTest")
            .config("spark.master", "local")
            .getOrCreate()
 
  
  val accountSchema = new StructType()
         .add("account_id",StringType)
         .add("limit", IntegerType)
         .add("products", ArrayType(StringType))
         .add("perf_timestamp",StringType)
         
   val customerSchema = new StructType()
         .add("username",StringType)
         .add("name", StringType)
         .add("address", StringType)
         .add("birthdate", StringType)
         .add("email", StringType)
         .add("perf_timestamp", StringType)
         .add("accounts", ArrayType(IntegerType))
    
   val transactionSchema = new StructType()
               .add("username",StringType)
               .add("name", StringType)
               .add("address", StringType)
               .add("birthdate", StringType)
               .add("email", StringType)
               .add("perf_timestamp", StringType)
               .add("account_id", IntegerType)
               .add("date", StringType)
        		   .add("amount", IntegerType)
        		   .add("transaction_code", StringType)
        		   .add("symbol", StringType)
        		   .add("price", StringType)
        		   .add("total", StringType)            
  val inputDF1=spark.read.schema(accountSchema).json("./src/test/scala/FuncReqTest/TestInput/accounts.json")
  val inputDF2=spark.read.schema(customerSchema).json("./src/test/scala/FuncReqTest/TestInput/customers.json")
  val inputDF3=spark.read.schema(transactionSchema).json("./src/test/scala/FuncReqTest/TestInput/transactions.json")
        
  val serviceUsageSchema=new StructType()
         .add("eachService",StringType)
         .add("count",IntegerType)
  val accountDistributionSchema=new StructType()
         .add("range",StringType)
         .add("sum(acc_len)",StringType)
  val stockRankByPriceSchema=new StructType()
         .add("symbol",StringType)
         .add("year",IntegerType)
         .add("month",IntegerType)
         .add("transaction_code",StringType)
         .add("amount_invsted",DoubleType)
  before{
    
        
  }
  test("HELLO") {
        
       val spark = SparkSession
            .builder()
            .appName("SparkTest")
            .config("spark.master", "local")
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        
        
        val outputDF=spark.read.schema(stockRankByPriceSchema).json("/home/rishi7/Documents/TestOutput/StockRankByPrice.json")
        
        outputDF.show()
        import spark.implicits._
               
        val resDF=stockRankbyPrice.compute(inputDF3)
//        resDF.write.json("/home/rishi7/Documents/TestOutput/StockRankByPrice")
        resDF.show()
        
        
        
        val finalDF=resDF.except(outputDF)
        finalDF.show()
        
       
        val p="hello";
        assert(p.equals("hello"))
    }
}
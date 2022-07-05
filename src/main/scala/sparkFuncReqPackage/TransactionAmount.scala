package sparkFuncReqPackage 

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

 object TransactionAmount {
    
     /** Calculate the total amount of transactions in each month
     *
     *  @param structured dateframe from transaction topic
     *  @return  DataFrame
     */
     def compute(df : DataFrame) : DataFrame ={
       
       // creating year and month field from transaction date
       val expandDF = df.withColumn("month", month(col("date")))
                        .withColumn("year", year(col("date")))
       
       val expand2DF = expandDF.select(col("month"), col("year"), col("total"))
    
       val aggregateDF = expand2DF.groupBy(col("month"), col("year")).agg(sum(col("total")))
       
       return aggregateDF
     }
 }

package sparkFuncReqPackage

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

object stockRankbyPrice {
  def compute(stockdf: DataFrame) : DataFrame = {
    // creating year and month field for the transactions
    val temp_df = stockdf.withColumn("year",  year(col("date")))
                .withColumn("month",month(col("date")));
    
    /* // query to see just output in console
     * val query1 = temp_df.writeStream
			  .format("console")
			  .outputMode("append")
			  .option("truncate", false)
			  .start() */
    
    // grouping "symbol", "year","month","transaction_code" and adding the total amount invested for that stock
    return temp_df.groupBy("symbol", "year","month","transaction_code")
        .agg(sum("total").as("amount_invsted"));
  }
}
package sparkFuncReqPackage

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

object VolatilityPattern {
  
     /** Analyse DataFrame to compute Volatility of a stock
   *
   *  @param DataFrame am Input DataFrame object
   *  @return  DataFrame computed DataFrame
   */
  
  def compute(stockdf : DataFrame) : DataFrame ={ 
           
           
               val temp_df = stockdf.withColumn("year", year(col("date")))
                          .withColumn("month", month(col("date")));

    val stockTradeDF = temp_df.filter(year(current_date()) - col("year")<11)//filter used to provide data of transactions withing 10 years
                              .groupBy("symbol", "year", "month")//group them on the basis of year and month to analyse seasonality
                              .count(); // count the result
    
    return stockTradeDF;
           
        }
}
package sparkFuncReqPackage

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

object ServiceUsage {
  def compute(df : DataFrame): DataFrame = 
    {

        val expandedDf=df.select(col("account_id"), explode(col("products")).alias("eachService"))
            
            expandedDf.groupBy("eachService")
                        .count();


      
    }
}
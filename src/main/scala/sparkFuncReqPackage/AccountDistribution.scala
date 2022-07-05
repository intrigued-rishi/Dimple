package sparkFuncReqPackage

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

object AccountDistribution {
  def compute(df : DataFrame): DataFrame = 
    {

        val ageCalculatedDF = df.withColumn("age", year(current_date())-year(col("birthdate")))
                      .withColumn("acc_len", size(col("accounts")))

        val interval = 10
        val rangeDividedDF = ageCalculatedDF.withColumn("range", col("age" )- (col("age") % interval))
                     .withColumn("range", concat(col("range"), lit(" - "), col("range") + interval)) 
        rangeDividedDF.groupBy(col("range"))
                      .agg(sum(col("acc_len")))


      
    }
}
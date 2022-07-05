import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._


package sparkFuncReqPackage
{


  object TransactionPattern {
      def compute(df : DataFrame) : DataFrame ={
           
          val yearExtractedDF = df.withColumn("date_year", year(col("date")))
          yearExtractedDF.groupBy("address","date_year")
                         .count()
                         .orderBy("address","date_year")
           
        }
    }
}
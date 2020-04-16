import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.{TimestampType, StringType, DoubleType, LongType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Main {

  //access log entry format fields

  val timestamp_col = "timestamp"
  val elb_col = "elb"
  val client_ip_port_col = "client_ip_port"
  val backend_ip_port_col = "backend_ip_port"
  val request_time_col = "request_time"
  val backend_time_col = "backend_time"
  val response_time_col = "response_time"
  val elb_status_code_col = "elb_status_code"
  val backend_status_code_col = "backend_status_code"
  val received_bytes_col = "received_bytes"
  val sent_bytes_col = "sent_bytes"
  val request_col = "request"
  val user_agent_col = "user_agent"
  val ssl_cipher_col = "ssl_cipher"
  val ssl_protocol_col = "ssl_protocol"

  //dervied fields
  val session_id_col = "session_id"
  val session_duration_in_seconds_col = "session_duration_in_seconds"
  val total_session_duration_in_seconds_col = "total_session_duration_in_seconds"
  val url_per_session_count_col = "url_per_session_count"
  val client_ip_col = "client_ip"

  val inactive_time_threshold_in_min = 15

  val log_schema = StructType(Array(
    StructField(timestamp_col, TimestampType, true),
    StructField(elb_col, StringType, true),
    StructField(client_ip_port_col, StringType, true),
    StructField(backend_ip_port_col, StringType, true),
    StructField(request_time_col, DoubleType, true),
    StructField(backend_time_col, DoubleType, true),
    StructField(response_time_col, DoubleType, true),
    StructField(elb_status_code_col, StringType, true),
    StructField(backend_status_code_col, StringType, true),
    StructField(received_bytes_col, LongType, true),
    StructField(sent_bytes_col, LongType, true),
    StructField(request_col, StringType, true),
    StructField(user_agent_col, StringType, true),
    StructField(ssl_cipher_col, StringType, true),
    StructField(ssl_protocol_col, StringType, true)
  ))

  def main(args: Array[String]): Unit = {
    val input = "/Users/gunpreetsingh/IdeaProjects/WebLogChallenge/2015_07_22_mktplace_shop_web_log_sample.log"
    val spark = SparkSession
      .builder()
      .appName("Weblog Challenge")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val logDf = read(spark, input)
    val sessionDf = sessionize(logDf)
    sessionDf.cache()
    sessionDf.show(25)

    println("**********" + " Average Duration Of All Sessions  " + "**********")
    sessionDf.agg(avg(col(session_duration_in_seconds_col))).show(false)

    val mostEngagedIpsDf: DataFrame = getEngagedIp(sessionDf)

    println("**********" + " Most Engaged IP's " + "**********")
    println(s"Number of most engaged IPs: ${mostEngagedIpsDf.count}")
    mostEngagedIpsDf.show()

    // unique urls visited per session
    val urlPerSessionDf = sessionDf.select(client_ip_port_col, user_agent_col, session_id_col, url_per_session_count_col)
    println("**********" + " URL's visited per session " + "**********")
    urlPerSessionDf.show()
    spark.close()
  }

  def getEngagedIp(sessionDf : DataFrame): DataFrame ={
    val sessionsTimeByIpDf = sessionDf.withColumn(client_ip_col, split(col(client_ip_port_col), ":")(0))
      .groupBy(client_ip_col)
      .agg(sum(session_duration_in_seconds_col).alias(total_session_duration_in_seconds_col)).cache()

    val longestSessionTime: Long = sessionsTimeByIpDf.select(max(col(total_session_duration_in_seconds_col))).take(1).head.getAs[Long](0)
    val mostEngagedIpsDf = sessionsTimeByIpDf.filter(col(total_session_duration_in_seconds_col) === longestSessionTime)
      .select(client_ip_col, total_session_duration_in_seconds_col)
    mostEngagedIpsDf
  }

  def sessionize(logDf: DataFrame): DataFrame = {
    val is_new_session_col = "is_new_session"
    val timestamp_long_col = "timestamp_long"
    val timeOrderedWin = Window.partitionBy(client_ip_port_col, user_agent_col).orderBy(timestamp_long_col)

    val dfWithSessionId = logDf
      .withColumn(timestamp_long_col, col(timestamp_col).cast("long"))
      .withColumn(is_new_session_col,
        (col(timestamp_long_col) - (lag(col(timestamp_long_col), offset = 1, defaultValue = 0).over(timeOrderedWin)) > (inactive_time_threshold_in_min * 60)).cast("long"))
      .withColumn(session_id_col, sum(is_new_session_col).over(timeOrderedWin))
      .drop(is_new_session_col)

    val sessionDf = dfWithSessionId.groupBy(client_ip_port_col, user_agent_col, session_id_col)
      .agg((max(timestamp_long_col) - min(timestamp_long_col)).alias(session_duration_in_seconds_col),
        countDistinct(request_col).alias(url_per_session_count_col))
      .filter(col(session_duration_in_seconds_col) =!= 0)
    sessionDf
  }

  def read(spark: SparkSession, input: String): DataFrame = {
    spark.read
      .option("delimiter", " ")
      .option("header", false)
      .schema(log_schema)
      .csv(input)
  }
}

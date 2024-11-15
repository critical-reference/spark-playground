package org.playground.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object Main extends App {

  val spark: SparkSession = SparkSession.builder().master("local[2]").getOrCreate()

  val yellowTripDataPath = "C:\\Users\\elena\\Desktop\\NYCdata\\yellow_tripdata_2024-02.parquet"
  // LOADING
  val yellow_tripdata: DataFrame = spark.read
    .parquet(yellowTripDataPath)
  val green_tripdata: DataFrame = spark.read
    .parquet("C:\\Users\\elena\\Desktop\\NYCdata\\green_tripdata_2024-02.parquet")
  val fhv_tripdata: DataFrame = spark.read
    .parquet("C:\\Users\\elena\\Desktop\\NYCdata\\fhv_tripdata_2024-02.parquet")
  val fhvhv_tripdata: DataFrame = spark.read
    .parquet("C:\\Users\\elena\\Desktop\\NYCdata\\fhvhv_tripdata_2024-02.parquet")


  avgs_yellow()
  avgs_green()
  weekly_avgs_yellow()
  weekly_avgs_green()
  hourly_avgs_green()
  hourly_avgs_yellow()
  most_traffic_yellow()
  most_traffic_green()
  avg_ticket_price()
  top_payment_methods()
  val (unusualTripsY, unusualTripsG) = passenger_analysis()
  unusualTripsY
    .write
    .mode("overwrite")
    .parquet("C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\unusual_trips_yellow")

  unusualTripsG
    .write
    .mode("overwrite")
    .parquet("C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\unusual_trips_green")


  def passenger_analysis(): (Dataset[Row], Dataset[Row]) = {

    val avgPassengerCountY = yellow_tripdata.agg(avg("passenger_count")).first().getDouble(0)
    println(s"Avg passengers Yellow: $avgPassengerCountY")
    val avgPassengerCountG = green_tripdata.agg(avg("passenger_count")).first().getDouble(0)
    println(s"Avg passengers Green: $avgPassengerCountG")


    val passengerStatsY = yellow_tripdata.agg(
      avg("passenger_count").alias("avg_passenger_count"),
      stddev("passenger_count").alias("stddev_passenger_count")
    ).first()

    val passengerStatsG = green_tripdata.agg(
      avg("passenger_count").alias("avg_passenger_count"),
      stddev("passenger_count").alias("stddev_passenger_count")
    ).first()


    val avgCountY = passengerStatsY.getDouble(0)
    val stdDevCountY = passengerStatsY.getDouble(1)

    val avgCountG = passengerStatsG.getDouble(0)
    val stdDevCountG = passengerStatsG.getDouble(1)

    val thresholdY = avgCountY + 2 * stdDevCountY
    println(s"Treshold Yellow: $thresholdY")

    val thresholdG = avgCountG + 2 * stdDevCountG
    println(s"Treshold Green: $thresholdG")

    val unusualTripsY = yellow_tripdata.filter(col("passenger_count") > thresholdY).orderBy(col("passenger_count").desc)
    val unusualTripsG = green_tripdata.filter(col("passenger_count") > thresholdG).orderBy(col("passenger_count").desc)

    (unusualTripsG, unusualTripsY)
  }

  def top_payment_methods() = {
    //yellow_tripdata.show()
    val payment_y = yellow_tripdata
      .groupBy("payment_type")
      .agg(count("payment_type"))
      .withColumnRenamed("count(payment_type)", "count")
      .orderBy(col("count").desc)

    payment_y
      .write
      .mode("overwrite")
      .parquet("C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\payment_methods_yellow")


    val payment_g = green_tripdata
      .groupBy("payment_type")
      .agg(count("payment_type"))
      .withColumnRenamed("count(payment_type)", "count")
      .orderBy(col("count").desc)

    payment_g
      .write
      .mode("overwrite")
      .parquet("C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\payment_methods_green")

  }

  def avg_ticket_price() = {
    val ticket_y = yellow_tripdata
      .agg(avg("total_amount"))
      .withColumnRenamed(("avg(total_amount)"), "yellow_price")
    val ticket_g = green_tripdata
      .agg(avg("total_amount"))
      .withColumnRenamed(("avg(total_amount)"), "green_price")

    val tickets = ticket_y.join(ticket_g)
    tickets.show()

    tickets
      .write
      .mode("overwrite")
      .parquet("C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\ticket_avgs")

    //ticket_y.show()
    //ticket_g.show()
  }

  def most_traffic_green() = {
    val PUloc = green_tripdata
      .groupBy("PULocationID")
      .agg(count("PULocationID"))
      .withColumnRenamed("count(PULocationID)", "pickup_count")
      .withColumnRenamed("PULocationID", "loc_id")
    //PUloc.show()

    val DOloc = green_tripdata
      .groupBy("DOLocationID")
      .agg(count("DOLocationID"))
      .withColumnRenamed("count(DOLocationID)", "dropoff_count")
      .withColumnRenamed("DOLocationID", "loc_id")

    //DOloc.show()

    val locs = DOloc.join(PUloc, "loc_id")
      .withColumn("total_count", col("pickup_count") + col("dropoff_count"))
      .orderBy(col("total_count").desc)

    val top10locs = locs.limit(10)
    top10locs
      .write
      .mode("overwrite")
      .parquet("C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\top_10_loc_green")

    top10locs.show()

  }

  def most_traffic_yellow() = {
    yellow_tripdata.show()
    val PUloc = yellow_tripdata
      .groupBy("PULocationID")
      .agg(count("PULocationID"))
      .withColumnRenamed("count(PULocationID)", "pickup_count")
      .withColumnRenamed("PULocationID", "loc_id")
    //PUloc.show()

    val DOloc = yellow_tripdata
      .groupBy("DOLocationID")
      .agg(count("DOLocationID"))
      .withColumnRenamed("count(DOLocationID)", "dropoff_count")
      .withColumnRenamed("DOLocationID", "loc_id")

    //DOloc.show()

    val locs = DOloc.join(PUloc, "loc_id")
      .withColumn("total_count", col("pickup_count") + col("dropoff_count"))
      .orderBy(col("total_count").desc)

    val top10locs = locs.limit(10)
    top10locs
      .write
      .mode("overwrite")
      .parquet("C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\top_10_loc_yellow")

    top10locs.show()

  }

  def hourly_avgs_yellow() = {
    val yellow_tripdata_with_hour = yellow_tripdata
      .withColumn("hour", hour(col("tpep_dropoff_datetime")))
      .withColumn(
        "duration_in_seconds",
        unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime")))

    val hourly_avgs_yell = yellow_tripdata_with_hour
      .groupBy("hour")
      .agg(
        avg("trip_distance"),
        avg("duration_in_seconds"))
      .alias("avg_distance")
      .orderBy("hour")

    hourly_avgs_yell
      .write
      .mode("overwrite")
      .parquet("C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\hourly_avgs_yellow")

    hourly_avgs_yell.show(28)

  }

  def hourly_avgs_green() = {
    val green_tripdata_with_hour = green_tripdata
      .withColumn("hour", hour(col("lpep_dropoff_datetime")))
      .withColumn(
        "duration_in_seconds",
        unix_timestamp(col("lpep_dropoff_datetime")) - unix_timestamp(col("lpep_pickup_datetime")))
    val hourly_avgs_gree = green_tripdata_with_hour
      .groupBy("hour")
      .agg(
        avg("trip_distance"),
        avg("duration_in_seconds"))
      .alias("avg_distance")
      .orderBy("hour")

    hourly_avgs_gree
      .write
      .mode("overwrite")
      .parquet("C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\hourly_avgs_green")

    hourly_avgs_gree.show(28)

    /*val check = green_tripdata.filter(hour(col("lpep_dropoff_datetime"))>19)
    check.show()*/
  }

  def weekly_avgs_green() = {
    val green_tripdata_with_day = green_tripdata
      .withColumn("day_of_week", date_format(col("lpep_dropoff_datetime"), "EEEE"))
      .withColumn("day_of_week_num", dayofweek(col("lpep_dropoff_datetime")))
      .withColumn(
        "duration_in_seconds",
        unix_timestamp(col("lpep_dropoff_datetime")) - unix_timestamp(col("lpep_pickup_datetime")))
    val weekly_avgs_gree = green_tripdata_with_day
      .groupBy("day_of_week", "day_of_week_num")
      .agg(
        avg("trip_distance"),
        avg("duration_in_seconds"))
      .alias("avg_distance")
      .withColumnRenamed("avg(trip_distance)", "trip_disance")
      .orderBy("day_of_week_num")

    weekly_avgs_gree
      .write
      .mode("overwrite")
      .parquet("C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\hourly_avgs_green")

    weekly_avgs_gree.show()
  }

  def weekly_avgs_yellow() = {
    val yellow_tripdata_with_day = yellow_tripdata
      .withColumn("day_of_week", date_format(col("tpep_dropoff_datetime"), "EEEE"))
      .withColumn("day_of_week_num", dayofweek(col("tpep_dropoff_datetime")))
      .withColumn(
        "duration_in_seconds",
        unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime")))
    val weekly_avgs_yell = yellow_tripdata_with_day
      .groupBy("day_of_week", "day_of_week_num")
      .agg(
        avg("trip_distance"),
        avg("duration_in_seconds"))
      .alias("avg_distance")
      .orderBy("day_of_week_num")

    weekly_avgs_yell
      .write
      .mode("overwrite")
      .parquet("C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\weekly_avgs_yellow")

    weekly_avgs_yell.show()
  }

  def avgs_yellow() = {
    yellow_tripdata.show();
    val avg_yell_dist: DataFrame = yellow_tripdata
      .agg(avg("trip_distance"))
      .withColumnRenamed("avg(trip_distance)", "avg_trip_distance")

    val avg_yell_duration = yellow_tripdata
      .withColumn(
        "duration_in_seconds",
        unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))).agg(avg("duration_in_seconds"))
      .withColumnRenamed("avg(duration_in_seconds)", "avg_duration_in_seconds")
    val avgs_yell = avg_yell_dist.join(avg_yell_duration)
    avgs_yell.show();
    avgs_yell
      .write
      .mode("overwrite")
      .parquet("C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\avgs_yellow")
  }

  def avgs_green() = {
    green_tripdata.show(100)
    val avg_gree_dist: DataFrame = green_tripdata
      .agg(avg("trip_distance"))
      .withColumnRenamed("avg(trip_distance)", "avg_trip_distance")

    val avg_gree_duration = green_tripdata
      .withColumn(
        "duration_in_seconds",
        unix_timestamp(col("lpep_dropoff_datetime")) - unix_timestamp(col("lpep_pickup_datetime"))).agg(avg("duration_in_seconds"))
      .withColumnRenamed("avg(duration_in_seconds)", "avg_duration_in_seconds")
    val avgs_gree = avg_gree_dist.join(avg_gree_duration)
    avgs_gree.show();
    avgs_gree
      .write
      .mode("overwrite")
      .parquet("C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\avgs_green")

    val t1 = green_tripdata.agg(avg("trip_distance")).first().getDouble(0)
    println(s"DISTANCE AVG $t1")
    val t2 = green_tripdata.filter(col("trip_distance") > 17)
    t2.show(100)


  }

}



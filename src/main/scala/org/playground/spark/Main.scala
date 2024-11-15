package org.playground.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object Main extends App {

  val spark: SparkSession = SparkSession.builder().master("local[2]").getOrCreate()

  // PATHS

  val yellowTripDataPath = "C:\\Users\\elena\\Desktop\\NYCdata\\yellow_tripdata_2024-02.parquet"
  val greenTripDataPath = "C:\\Users\\elena\\Desktop\\NYCdata\\green_tripdata_2024-02.parquet"

  val unusualTripsYellowPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\unusual_trips_yellow"
  val unusualTripsGreenPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\unusual_trips_green"

  val paymentMethodsYellowPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\payment_methods_yellow"
  val paymentMethodsGreenPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\payment_methods_green"

  val ticketAveragesPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\ticket_avgs"

  val top10LocsGreenPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\top_10_loc_green"
  val top10LocsYellowPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\top_10_loc_yellow"

  val hourlyAveragesYellowPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\hourly_avgs_yellow"
  val hourlyAveragesGreenPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\hourly_avgs_green"

  val weeklyAvgsYellowPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\weekly_avgs_yellow"
  val weeklyAvgsGreenPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\weekly_avgs_green"

  val averagesYellowPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\avgs_yellow"
  val averagesGreenPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\avgs_green"


  // LOADING
  val yellow_tripdata: DataFrame = spark.read
    .parquet(yellowTripDataPath)
  val green_tripdata: DataFrame = spark.read
    .parquet(greenTripDataPath)


  // MAIN

  val avgTicketPrices = avg_ticket_price(yellow_tripdata, green_tripdata)
   writeToParquet(avgTicketPrices, ticketAveragesPath)

  val (paymentMethodsYellow, paymentMethodsGreen ) = top_payment_methods(yellow_tripdata, green_tripdata)
  writeToParquet(paymentMethodsYellow, paymentMethodsYellowPath)
  writeToParquet(paymentMethodsGreen, paymentMethodsGreenPath)

  val (unusualTripsY, unusualTripsG) = passenger_analysis(yellow_tripdata, green_tripdata)
  writeToParquet(unusualTripsY, unusualTripsYellowPath)
  writeToParquet(unusualTripsG, unusualTripsGreenPath)

  val top10LocsYellow = most_traffic(yellow_tripdata)
  writeToParquet(top10LocsYellow, top10LocsYellowPath)
  val top10LocsGreen = most_traffic(green_tripdata)
  writeToParquet(top10LocsGreen, top10LocsGreenPath)

  var hourlyAveragesYellow = hourly_avgs_yellow(yellow_tripdata)
  writeToParquet(hourlyAveragesYellow, hourlyAveragesYellowPath)
  var hourlyAveragesGreen = hourly_avgs_green(green_tripdata)
  writeToParquet(hourlyAveragesGreen, hourlyAveragesGreenPath)

  var weeklyAveragesYellow = weekly_avgs_yellow(yellow_tripdata)
  writeToParquet(weeklyAveragesYellow, weeklyAvgsYellowPath)
  var weeklyAveragesGreen = weekly_avgs_green(green_tripdata)
  writeToParquet(weeklyAveragesGreen, weeklyAvgsGreenPath)

  var averagesYellow = avgs_yellow(yellow_tripdata)
  writeToParquet(averagesYellow, averagesYellowPath)
  var averagesGreen = avgs_green(green_tripdata)
  writeToParquet(averagesGreen, averagesGreenPath)

  // FUNCTIONS

  def writeToParquet(df: Dataset[Row], path: String)={
    df
      .write
      .mode("overwrite")
      .parquet(path)
  }

  def passenger_analysis(yellow: Dataset[Row], green: Dataset[Row]): (Dataset[Row], Dataset[Row]) = {

    val avgPassengerCountY = yellow.agg(avg("passenger_count")).first().getDouble(0)
    println(s"Avg passengers Yellow: $avgPassengerCountY")
    val avgPassengerCountG = green.agg(avg("passenger_count")).first().getDouble(0)
    println(s"Avg passengers Green: $avgPassengerCountG")


    val passengerStatsY = yellow.agg(
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

    val unusualTripsY = yellow.filter(col("passenger_count") > thresholdY).orderBy(col("passenger_count").desc)
    val unusualTripsG = green.filter(col("passenger_count") > thresholdG).orderBy(col("passenger_count").desc)

    (unusualTripsG, unusualTripsY)
  }

  def top_payment_methods(yellow: Dataset[Row], green: Dataset[Row]): (Dataset[Row], Dataset[Row]) = {

    val payment_y = yellow
      .groupBy("payment_type")
      .agg(count("payment_type"))
      .withColumnRenamed("count(payment_type)", "count")
      .orderBy(col("count").desc)

    val payment_g = green
      .groupBy("payment_type")
      .agg(count("payment_type"))
      .withColumnRenamed("count(payment_type)", "count")
      .orderBy(col("count").desc)

    (payment_y,payment_g)
  }

  def avg_ticket_price(yellow: Dataset[Row], green: Dataset[Row]): (Dataset[Row]) = {
    val ticket_y = yellow
      .agg(avg("total_amount"))
      .withColumnRenamed(("avg(total_amount)"), "yellow_price")
    val ticket_g = green
      .agg(avg("total_amount"))
      .withColumnRenamed(("avg(total_amount)"), "green_price")

    val tickets = ticket_y.join(ticket_g)
    tickets.show()

    tickets
  }

  def most_traffic(ds: Dataset[Row]):Dataset[Row] = {
    val PUloc = ds
      .groupBy("PULocationID")
      .agg(count("PULocationID"))
      .withColumnRenamed("count(PULocationID)", "pickup_count")
      .withColumnRenamed("PULocationID", "loc_id")

    val DOloc = ds
      .groupBy("DOLocationID")
      .agg(count("DOLocationID"))
      .withColumnRenamed("count(DOLocationID)", "dropoff_count")
      .withColumnRenamed("DOLocationID", "loc_id")

    val locs = DOloc.join(PUloc, "loc_id")
      .withColumn("total_count", col("pickup_count") + col("dropoff_count"))
      .orderBy(col("total_count").desc)

    val top10locs = locs.limit(10)

    top10locs

  }

  def hourly_avgs_yellow(yellow: Dataset[Row]):Dataset[Row] = {
    val yellow_tripdata_with_hour = yellow
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
  }

  def hourly_avgs_green(green: Dataset[Row]):Dataset[Row] = {
    val green_tripdata_with_hour = green
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
  }

  def weekly_avgs_yellow(yellow: Dataset[Row]): Dataset[Row] = {
    val yellow_tripdata_with_day = yellow
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
  }

  def weekly_avgs_green(green: Dataset[Row]): Dataset[Row] = {
    val green_tripdata_with_day = green
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
  }

  def avgs_yellow(yellow: Dataset[Row]): Dataset[Row] = {
    val avg_yell_dist: DataFrame = yellow
      .agg(avg("trip_distance"))
      .withColumnRenamed("avg(trip_distance)", "avg_trip_distance")

    val avg_yell_duration = yellow
      .withColumn(
        "duration_in_seconds",
        unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))).agg(avg("duration_in_seconds"))
      .withColumnRenamed("avg(duration_in_seconds)", "avg_duration_in_seconds")
    val avgs_yell = avg_yell_dist.join(avg_yell_duration)

    avgs_yell

  }

  def avgs_green(green: Dataset[Row]): Dataset[Row] = {
    val avg_gree_dist: DataFrame = green
      .agg(avg("trip_distance"))
      .withColumnRenamed("avg(trip_distance)", "avg_trip_distance")

    val avg_gree_duration = green
      .withColumn(
        "duration_in_seconds",
        unix_timestamp(col("lpep_dropoff_datetime")) - unix_timestamp(col("lpep_pickup_datetime"))).agg(avg("duration_in_seconds"))
      .withColumnRenamed("avg(duration_in_seconds)", "avg_duration_in_seconds")
    val avgs_gree = avg_gree_dist.join(avg_gree_duration)

    val t1 = green.agg(avg("trip_distance")).first().getDouble(0)
    println(s"DISTANCE AVG $t1")
    val t2 = green.filter(col("trip_distance") > 17)
    t2.show(100)

    avgs_gree
  }

}



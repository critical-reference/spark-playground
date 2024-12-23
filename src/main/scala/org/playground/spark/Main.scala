package org.playground.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, unix_timestamp, _}
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.sql.Date
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDate, LocalDateTime, Month}

object Main extends App {

  val spark: SparkSession = SparkSession.builder().master("local[2]").getOrCreate()
  val sc = spark.sparkContext


  // PATHS

  val yellowTripDataPath = "C:\\Users\\elena\\Desktop\\NYCdata\\yellow_tripdata_2024-02.parquet"
  val greenTripDataPath = "C:\\Users\\elena\\Desktop\\NYCdata\\green_tripdata_2024-02.parquet"
  val weatherDataPath = "C:\\Users\\elena\\Desktop\\NYCdata\\open-meteo-40.74N74.04W27m.csv"
  val zoneDataPath = "C:\\Users\\elena\\Desktop\\NYCdata\\taxi_zones.csv"
  val dateFormatter = DateTimeFormatter.ofPattern("M/d/yyyy")

  val yellowPickupColumn = "tpep_pickup_datetime"
  val yellowDropoffColumn = "tpep_dropoff_datetime"
  val greenPickupColumn = "lpep_pickup_datetime"
  val greenDropoffColumn = "lpep_dropoff_datetime"

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

  val ridesByPaymentYellowPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\rides_by_paymeny_yellow"
  val ridesByPaymentGreenPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\rides_by_payment_green"

  val ridesByHourYellowPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\rides_by_hour_yellow"
  val ridesByHourGreenPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\rides_by_hour_green"

  val maxTipByVendorYellowPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\max_tip_by_vendor_yellow"
  val maxTipByVendorGreenPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\max_tip_by_vendor_green"

  val totalFaresByDayYellowPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\fares_by_day_yellow"
  val totalFaresByDayGreenPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\fares_by_day_green"

  val ridesWithWeatherYellowPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\rides_with_weather_yellow"
  val ridesWithWeatherGreenPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\rides_with_weather_green"

  val ridesWithZoneNamesYellowPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\rides_with_zone_names_yellow"
  val ridesWithZoneNamesGreenPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\rides_with_zone_names_green"

  val peakHoursYellowPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\peak_hour_yellow"
  val peakHoursGreenPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\peak_hour_green"

  val tipsByZoneYellowPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\tips_by_zone_yellow"
  val tipsByZoneGreenPath = "C:\\Users\\elena\\IdeaProjects\\spark-playground\\outputs\\tips_by_zone_green"


  // LOADING
  val yellow_tripdata: DataFrame = spark.read
    .parquet(yellowTripDataPath)
  val green_tripdata: DataFrame = spark.read
    .parquet(greenTripDataPath)

  val yellow_schema = StructType(yellow_tripdata.schema.fields)
  val green_schema = StructType(green_tripdata.schema.fields)

  val weather_data: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(weatherDataPath)

  val zone_data: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(zoneDataPath)

  zone_data.show()



  // MAIN2

  val avgTicketPrices = avg_ticket_price(yellow_tripdata, green_tripdata)
  writeToParquet(avgTicketPrices, ticketAveragesPath)

  val (paymentMethodsYellow, paymentMethodsGreen) = top_payment_methods(yellow_tripdata, green_tripdata)
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


  val yellow_rdd = yellow_tripdata.rdd
  val green_rdd = green_tripdata.rdd
  val weather_rdd = weather_data.rdd //.zipWithIndex().filter{ case (_, index) => index>=2 }.map(_._1)
  val zones_rdd = zone_data
    .rdd
    .map(row => {
      val col1 = row.getAs[String]("zone")
      val col2 = row.getAs[Number]("LocationID")
      (col1, col2)
    })


  val yellow_rides = countRides(yellow_rdd)
  val green_rides = countRides(green_rdd)
  println(s"Total rides yellow $yellow_rides")
  println(s"Total rides green $green_rides")

  val yellow_times = earliest_latest_ride(yellow_rdd, yellowPickupColumn, yellowDropoffColumn)
  println(s"Yellow earliest pickup: ${yellow_times._1}")
  println(s"Yellow latest dropoff: ${yellow_times._2}")

  val green_times = earliest_latest_ride(green_rdd, greenPickupColumn, greenDropoffColumn)
  println(s"Green earliest pickup: ${green_times._1}")
  println(s"Green latest dropoff: ${green_times._2}")

  val yellow_passenger_count = passenger_count(yellow_rdd)
  println(s"Yellow total passengers: ${yellow_passenger_count}")
  val green_passenger_count = passenger_count(green_rdd)
  println(s"Green total passengers: ${green_passenger_count}")

  val yellow_short_rides = short_rides_count(yellow_rdd)
  println(s"Yellow short rides count: ${yellow_short_rides}")
  val green_short_rides = short_rides_count(green_rdd)
  println(s"Green short rides count: ${green_short_rides}")

  val yellow_rides_per_type = rides_per_type(yellow_rdd, ridesByPaymentYellowPath)
  yellow_rides_per_type.collect().foreach(println)

  val green_rides_per_type = rides_per_type(green_rdd, ridesByPaymentGreenPath)
  green_rides_per_type.collect().foreach(println)

  val yellow_shortest_longest_ride = shortest_longest_ride(yellow_rdd)
  println(s"Yellow shortest ride: ${yellow_shortest_longest_ride._1}")
  println(s"Yellow longest_ride: ${yellow_shortest_longest_ride._2}")

  val green_shortest_longest_ride = shortest_longest_ride(green_rdd)
  println(s"Green shortest ride: ${green_shortest_longest_ride._1}")
  println(s"Green longest_ride: ${green_shortest_longest_ride._2}")

  val yellow_average_speed = average_duration(yellow_rdd, yellowPickupColumn, yellowDropoffColumn)
  println(s"Yellow average speed: ${yellow_average_speed} miles/hour")
  val green_average_speed = average_duration(green_rdd, greenPickupColumn, greenDropoffColumn)
  println(s"Green average speed: ${green_average_speed} miles/hour")

  val yellow_rides_per_hour = rides_per_hour(yellow_rdd, yellowPickupColumn, ridesByHourYellowPath)
  yellow_rides_per_hour.collect().foreach(println)
  val green_rides_per_hour = rides_per_hour(green_rdd, greenPickupColumn, ridesByHourGreenPath)
  green_rides_per_hour.collect().foreach(println)

  val yellow_max_tip_by_vendor = max_tip_by_vendor(yellow_rdd, maxTipByVendorYellowPath)
  yellow_max_tip_by_vendor.collect().foreach(println)
  val green_max_tip_by_vendor = max_tip_by_vendor(green_rdd, maxTipByVendorGreenPath)
  green_max_tip_by_vendor.collect().foreach(println)

  val yellow_fares_by_day = fares_by_day(yellow_rdd, yellowDropoffColumn, totalFaresByDayYellowPath)
  yellow_fares_by_day.collect().foreach(println)
  val green_fares_by_day = fares_by_day(green_rdd, greenDropoffColumn, totalFaresByDayGreenPath)
  green_fares_by_day.collect().foreach(println)

  val yellow_taxi_weather = join_with_weather(weather_rdd, yellow_rdd, yellowPickupColumn, ridesWithWeatherYellowPath)
  yellow_taxi_weather.collect().foreach(println)

  val green_taxi_weather = join_with_weather(weather_rdd, green_rdd, greenPickupColumn, ridesWithWeatherGreenPath)
  green_taxi_weather.collect().foreach(println)

  val yellow_with_zones = with_zones_pu_do(yellow_rdd, zones_rdd, yellow_schema, ridesWithZoneNamesYellowPath)
  val green_with_zones = with_zones_pu_do(green_rdd, zones_rdd, green_schema, ridesWithZoneNamesGreenPath)


  val yellow_peak_hour = get_peak_hours(yellow_rdd, yellowDropoffColumn, peakHoursYellowPath)
  yellow_peak_hour.collect().foreach(println)

  val green_peak_hour = get_peak_hours(green_rdd, greenDropoffColumn, peakHoursGreenPath)
  green_peak_hour.collect().foreach(println)

  val yellow_tips_by_zone = tips_by_zone(yellow_rdd, zones_rdd, tipsByZoneYellowPath)
  yellow_tips_by_zone.collect().foreach(println)

  val green_tips_by_zone = tips_by_zone(green_rdd, zones_rdd, tipsByZoneGreenPath)
  green_tips_by_zone.collect().foreach(println)


  // FUNCTIONS


  def tips_by_zone(rides_rdd: RDD[Row], zones_rdd: RDD[(String, Number)], path: String): RDD[(Int, String, Double)] = {
    val keyedRides = rides_rdd.map(row => {
      val col1 = row.getAs[Int]("PULocationID")
      val col2 = row.getAs[Double]("tip_amount")
      (col1, col2)
    }).reduceByKey(_ + _)

    val keyedZones = zones_rdd.map { case (zoneName, zoneId) => (zoneId.asInstanceOf[Int], zoneName) }

    val joinedRDD = keyedRides.leftOuterJoin(keyedZones)

    val zones_with_tips = joinedRDD.map {
      case (id, (tipAmount, zoneNameOption)) =>
        val zoneName = zoneNameOption.orNull
        (id.asInstanceOf[Int], zoneName, tipAmount)
    }

    val zones_with_tips_sorted = zones_with_tips.sortBy(_._1)


    val schema = StructType(List(
      StructField("zone_id", IntegerType, nullable = true),
      StructField("zone_name", StringType, nullable = true),
      StructField("total_tips", DoubleType, nullable = true),
    ))

    val df = spark.createDataFrame(
      zones_with_tips_sorted.map {
        case (id, name, total) => Row(id, name, total)
      }, schema)

    writeToParquet(df, path)

    val df1 = spark.read.parquet(path)
    df1.show()
    zones_with_tips_sorted

  }


  def get_peak_hours(rides: RDD[Row], column: String, path: String): RDD[(Int, Int)] = {
    val r1: RDD[(Int, Int)] = rides
      .map(row => {
        val col1 = row.getAs[java.time.LocalDateTime](column).toLocalTime.getHour.intValue()
        val col2 = 1
        (col1, col2)
      }).reduceByKey(_ + _).sortBy(_._2, ascending = false)

    val schema = StructType(List(
      StructField("hour", IntegerType, nullable = true),
      StructField("total_rides", IntegerType, nullable = true),
    ))

    val df = spark.createDataFrame(
      r1.map {
        case (hour, cnt) => Row(hour, cnt)
      }, schema)

    writeToParquet(df, path)

    val df1 = spark.read.parquet(path)
    df1.show()

    r1
  }


  def with_zones_pu_do(rides_rdd: RDD[Row], zones_rdd: RDD[(String, Number)], schema: StructType, path: String) = {

    val keyedRides = rides_rdd.keyBy { rideRow =>
      Option(rideRow.getAs[Int]("PULocationID")).getOrElse(0)
    }
    val keyedZones = zones_rdd.map { case (zoneName, zoneId) => (zoneId.asInstanceOf[Int], zoneName) }

    val joinedRDD = keyedRides.leftOuterJoin(keyedZones)

    val with_pu_rdd = joinedRDD.map {
      case (_, (rideRow, zoneNameOption)) =>
        val zoneName = zoneNameOption.getOrElse("")
        val rideData = rideRow.toSeq :+ zoneName
        Row.fromSeq(rideData)
    }

    val with_pu_schema = StructType(schema
      :+ StructField("PULocationName", StringType, nullable = true))

    val with_pu_df = spark.createDataFrame(with_pu_rdd, with_pu_schema)

    with_pu_df.show()

    val keyedRides2 = with_pu_df.rdd.keyBy { rideRow =>
      Option(rideRow.getAs[Int]("DOLocationID")).getOrElse(0)
    }

    val joinedRDD2 = keyedRides2.leftOuterJoin(keyedZones)

    val with_pu_do_rdd = joinedRDD2.map {
      case (_, (rideRow, zoneNameOption)) =>
        val zoneName = zoneNameOption.getOrElse("")
        val rideData = rideRow.toSeq :+ zoneName
        Row.fromSeq(rideData)
    }

    val with_pu_do_schema = StructType(schema
      :+ StructField("PULocationName", StringType, nullable = true)
      :+ StructField("DOLocationName", StringType, nullable = true))

    val with_pu_do = spark.createDataFrame(with_pu_do_rdd, with_pu_do_schema)

    writeToParquet(with_pu_do, path)
    val df1 = spark.read.parquet(path)
    df1.show()

  }

  def join_with_weather(weather: RDD[Row], rides: RDD[Row], pickup_column: String, path: String) = {
    val rides_per_day: RDD[(Date, Int)] = rides
      .map(row => {
        val pickupTime = row.getAs[java.time.LocalDateTime](pickup_column)
        val localDate = Date.valueOf(pickupTime.toLocalDate) // java.sql.Date
        (localDate, 1)
      })
      .reduceByKey(_ + _)

    val weather_per_day = weather
      .map(row => {
        val col1 = row.getAs[String]("time")
        val date = try {
          Some(Date.valueOf(LocalDate.parse(col1, dateFormatter)))
        } catch {
          case e: Exception => None
        }
        val col2 = row.getAs[Number]("precipitation_sum (mm)")
        val col3 = row.getAs[Number]("temperature_2m_mean (°C)")

        date.map(d => (d, (col2, col3)))
      })
      .filter(_.isDefined)
      .map(_.get)


    val joined_data = rides_per_day.join(weather_per_day)

    val final_data = joined_data
      .map { case (date, (ridesCount, (precipitation, temperature))) =>
        val precip = if (precipitation != null) precipitation.doubleValue() else 0.0
        val temp = if (temperature != null) temperature.doubleValue() else 0.0
        (date, ridesCount.intValue(), precip, temp)
      }
      //.sortBy(x => x._1.getDate)
      .sortBy(x => x._2)


    val schema = StructType(List(
      StructField("date", DateType, nullable = true),
      StructField("total_rides", IntegerType, nullable = true),
      StructField("precipitation_sum (mm)", DoubleType, nullable = true),
      StructField("temperature_2m_mean (°C)", DoubleType, nullable = true)
    ))

    val df = spark.createDataFrame(
      final_data.map {
        case (date, cnt, pre, temp) => Row(date, cnt, pre, temp)
      }, schema)

    writeToParquet(df, path)

    val df1 = spark.read.parquet(path)
    df1.show()

    final_data
  }

  def fares_by_day(rides: RDD[Row], column: String, path: String): RDD[(LocalDate, Double)] = {
    val fares: RDD[(LocalDate, Double)] = rides
      .map(row => (row.getAs[java.time.LocalDateTime](column).toLocalDate, row.getAs[Number]("fare_amount").doubleValue()))
      .reduceByKey(_ + _)
    val fares_sorted = fares.filter(x => Date.valueOf(x._1).getMonth == 1 && Date.valueOf(x._1).getYear == 2024 - 1900).sortBy(x => x._1.getDayOfMonth)


    val schema = StructType(List(
      StructField("date", DateType, nullable = true),
      StructField("total_fares", DoubleType, nullable = true)
    ))

    val df = spark.createDataFrame(
      fares_sorted.map {
        case (date, amount) => Row(Date.valueOf(date), amount)
      }, schema)

    writeToParquet(df, path)

    val df1 = spark.read.parquet(path)
    df1.show()

    fares_sorted


  }

  def max_tip_by_vendor(rides: RDD[Row], path: String) = {
    val tips: RDD[(Long, Double)] = rides
      .map(row => (row.getAs[Number]("VendorID").longValue(), row.getAs[Number]("tip_amount").doubleValue()))
      .reduceByKey((a, b) => math.max(a, b))

    val schema = StructType(List(
      StructField("vendor_id", LongType, nullable = true),
      StructField("max_tip", DoubleType, nullable = true)
    ))

    val df = spark.createDataFrame(
      tips.map {
        case (id, m) => Row(id, m)
      }, schema)

    writeToParquet(df, path)

    val df1 = spark.read.parquet(path)
    df1.show()

    tips
  }

  def rides_per_hour(rides: RDD[Row], pickup_column: String, path: String): RDD[(Long, Int)] = {
    val rides_per_hour = rides
      .map(row => {
        (row.getAs[java.time.LocalDateTime](pickup_column).getHour.toLong, 1)
      })
      .reduceByKey(_ + _)
    val rides_per_hour_sorted = rides_per_hour.sortBy(x => x._1)

    val schema = StructType(List(
      StructField("hour", LongType, nullable = true),
      StructField("rides_count", IntegerType, nullable = true)
    ))

    val df = spark.createDataFrame(
      rides_per_hour_sorted.map {
        case (hour, count) => Row(hour, count)
      }, schema)

    writeToParquet(df, path)

    val df1 = spark.read.parquet(path)
    df1.show()

    rides_per_hour_sorted
  }

  def average_duration(rides: RDD[Row], pickup_column: String, dropoff_column: String): Double = {
    val rides_formatted: RDD[(LocalDateTime, LocalDateTime, Double)] = rides.map(row => {
      val col1 = row.getAs[java.time.LocalDateTime](pickup_column)
      val col2 = row.getAs[java.time.LocalDateTime](dropoff_column)
      val col3 = Option(row.getAs[Number]("trip_distance"))
        .map(x => x.doubleValue())
        .getOrElse(0.0)
      (col1, col2, col3)
    })
    val with_differences = rides_formatted.map(row => {
      val duration = Duration.between(row._1, row._2).getSeconds.doubleValue
      (duration, row._3)
    })
    val zero_val = (0.0, 0.0)
    val final_sum = with_differences.aggregate(zero_val)(
      (acc, value) => (acc._1 + value._1, acc._2 + value._2),
      (acc1, acc2) => ((acc1._1 + acc2._1), acc1._2 + acc2._2)
    )
    println(s"Total duration: ${final_sum._1}seconds Total distance: ${final_sum._2}miles");
    (final_sum._2 / final_sum._1) * 3600
  }

  def shortest_longest_ride(rides: RDD[Row]): (Double, Double) = {
    val distances: RDD[Double] = rides.map(row => {
      Option(row.getAs[Number]("trip_distance"))
        .map(x => x.doubleValue())
        .getOrElse(0)
    })
    val zeroVal = (Double.MaxValue, Double.MinValue)
    val min_max = distances.aggregate(zeroVal)(
      (acc, value) => (math.min(acc._1, value), math.max(acc._2, value)),
      (acc1, acc2) => (math.min(acc1._1, acc2._1), math.max(acc1._2, acc2._2))
    )
    min_max
  }

  def rides_per_type(rides: RDD[Row], path: String): RDD[(Number, Int)] = {
    val payment_type_count = rides
      .map(row => (row.getAs[Number]("payment_type"), 1))
      .reduceByKey((v1, v2) => v1 + v2)
    //val sorted = payment_type_count.sortBy(_._1, ascending = false)(Ordering[Int])


    val schema = StructType(List(
      StructField("payment_type", LongType, nullable = true),
      StructField("rides_count", IntegerType, nullable = true)
    ))

    val df = spark.createDataFrame(
      payment_type_count.map {
        case (paymentType, count) => Row(paymentType, count)
      }, schema)

    writeToParquet(df, path)

    val df1 = spark.read.parquet(path)
    df1.show()

    payment_type_count
  }

  def short_rides_count(rides: RDD[Row]): Number = {
    val distances: RDD[Double] = rides.map(row => {
      Option(row.getAs[Number]("trip_distance"))
        .map(x => x.doubleValue())
        .getOrElse(0)
    })
    val short_distances = distances.filter(d => d > 1)
    short_distances.count()
  }

  def passenger_count(rides: RDD[Row]): Number = {
    val passengers = rides.map(row => {
      Option(row.getAs[Number]("passenger_count"))
        .map(x => x.intValue())
        .getOrElse(0)
    })
    val zeroValue = 0
    val total_count = passengers.aggregate(zeroValue)(
      (acc, value) => acc + value,
      (acc1, acc2) => acc1 + acc2
    )
    total_count
  }

  def earliest_latest_ride(rides: RDD[Row], pickup_column: String, dropoff_column: String) = {
    val rides_formatted = rides.map(row => {
      val col1 = row.getAs[java.time.LocalDateTime](pickup_column)
      val col2 = row.getAs[java.time.LocalDateTime](dropoff_column)
      (col1, col2)
    })
    val zero_value = (java.time.LocalDateTime.MAX, java.time.LocalDateTime.MIN) // (min, max)
    val min_max_time = rides_formatted.aggregate(zero_value)(
      (acc, value) => (earlier_time(acc._1, value._1), later_time(acc._2, value._2)), // SeqOp
      (acc1, acc2) => (earlier_time(acc1._1, acc2._1), later_time(acc1._2, acc2._2)) // CombOp
    )
    min_max_time
  }

  def earlier_time(d1: java.time.LocalDateTime, d2: java.time.LocalDateTime): java.time.LocalDateTime = {
    if (d1.toLocalTime.isBefore(d2.toLocalTime))
      return d1
    else return d2
  }

  def later_time(d1: java.time.LocalDateTime, d2: java.time.LocalDateTime): java.time.LocalDateTime = {
    if (d1.toLocalTime.isAfter(d2.toLocalTime))
      return d1
    else return d2
  }


  def countRides(rides: RDD[Row]): Number = {
    val t = rides.count()
    t
  }

  def exampleFunction() {
    // Sample dataset: List of ride IDs
    val rides: RDD[String] = sc.parallelize(Seq("ride1", "ride2", "ride3", "ride4", "ride5"))
    // Count the total number of rides
    val totalRides = rides.count();
    println(s"Total number of rides $totalRides")
    // Sample dataset: List of ride distances
    val distances: RDD[Double] = sc.parallelize(Seq(2.3, 7.5, 1.2, 4.0, 5.8))
    // Using aggregate to find min and max distances
    val zeroValue = (Double.MaxValue, Double.MinValue) // (min, max)
    val minMaxDistances = distances.aggregate(zeroValue)(
      (acc, value) => (math.min(acc._1, value), math.max(acc._2, value)), // SeqOp
      (acc1, acc2) => (math.min(acc1._1, acc2._1), math.max(acc1._2, acc2._2)) // CombOp
    )
    println(s"Shortest ride distance: ${minMaxDistances._1}")
    println(s"Longest ride distance: ${minMaxDistances._2}")

    val rideData = sc.parallelize(Seq(
      ("zone1", "ride1"),
      ("zone2", "ride2"),
      ("zone3", "ride3")
    )) // (pickup_zone, ride_id)
    val zoneData = sc.parallelize(Seq(
      ("zone1", "Downtown"),
      ("zone2", "Midtown"),
      ("zone3", "Uptown")
    )) // (zone_id, zone_name)
    // Perform an inner join on the key (zone_id)
    val joinedData = rideData.join(zoneData)
    // Result: (zone_id, (ride_id, zone_name))
    joinedData.collect().foreach { case (zone, (rideId, zoneName)) =>
      println(s"Ride ID: $rideId, Zone Name: $zoneName")
    }
  }

  def writeToParquet(df: Dataset[Row], path: String) = {
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

    (payment_y, payment_g)
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

  def most_traffic(ds: Dataset[Row]): Dataset[Row] = {
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

  def hourly_avgs_yellow(yellow: Dataset[Row]): Dataset[Row] = {
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

  def hourly_avgs_green(green: Dataset[Row]): Dataset[Row] = {
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




import org.apache.spark.sql.{SparkSession, DataFrame}
import java.io._
import org.apache.spark.sql.functions._
import util.Try


object App {

  def part1(df_UR: DataFrame): DataFrame = {
    //Replace records where the column 'Sentiment_Polarity'==nan with '0'
    var cleanedDF_UR = df_UR.withColumn("Sentiment_Polarity", when(col("Sentiment_Polarity") === "nan", lit(0)).otherwise(col("Sentiment_Polarity")))
    //cleanedDF_UR.show()

    // Change 'Sentiment_Polarity' datatype from String to double
    cleanedDF_UR = cleanedDF_UR.withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double"))
    //cleanedDF_UR.printSchema()

    //Select the columns 'App' and 'Sentiment_Polarity'
    cleanedDF_UR = cleanedDF_UR.select("App", "Sentiment_Polarity")
    //cleanedDF_UR.show()

    // Group by 'App' and replace 'Sentiment_Polarity' with Average_Sentiment_Polarity (Average of the column Sentiment_Polarity grouped by App name)
    val df_1 = cleanedDF_UR.groupBy("App").agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    df_1
  }


  def part2(df_PS: DataFrame) = {

    // Filter records where the first character of 'Category' is anuppercase letter (Remove wrong records)
    var filteredDF_PS = df_PS.filter(col("Category").rlike("^[A-Z]"))

    // Delete records where 'Rating'==NaN
    filteredDF_PS = filteredDF_PS.filter("Rating != 'NaN'")

    // Get records where 'Rating' contains only numbers
    filteredDF_PS = filteredDF_PS.filter(col("Rating").rlike("^\\d*\\.?\\d+$"))

    // Change 'Rating' datatype from String to double
    filteredDF_PS = filteredDF_PS.withColumn("Rating", col("Rating").cast("double"))

    // Get records where 'Rating'>=4.0
    var res = filteredDF_PS.filter(col("Rating") >= 4.0)

    // Order df by rating in descending order
    res = res.sort(col("Rating").desc)

    val directoryPath = "src/main/resources/datasets/Results/Part2"

    // Save DataFrame as CSV file with separator '§'
    res.repartition(1)
      .write
      .option("header", true)
      .option("encoding", "UTF-8")
      .option("sep", "§")
      .mode("overwrite")
      .csv(directoryPath)

    // Change CSV name
    // Get a list of files in the directory
    val files = new File(directoryPath).listFiles()

    // Filter CSV files
    var csvFiles = files.filter(_.getName.toLowerCase.endsWith(".csv"))

    val csvFile = csvFiles.head.toString

    Try(new File(csvFile).renameTo(new File("src/main/resources/datasets/Results/Part2/best_apps.csv"))).getOrElse(false)
  }



  def part3(df_PS: DataFrame): DataFrame = {

    // UDF to 'Size from String to double in MB
    val size_transformation = udf((size: String) => {
      // Extract the numeric part of 'Size'
      val numericSize = "(\\d+\\.?\\d*)".r.findFirstIn(size).getOrElse("-1").toDouble
      var flag = 0
      var res = (-1).toDouble

      // Check if the size is in MB
      if (size.toLowerCase.endsWith("m")) {
        flag=1
        res = numericSize
      }
      // Check if the size is in kB and convert
      if (size.toLowerCase.endsWith("k")) {
        flag=1
        res = numericSize * 0.001
      }
      // If numericSize == -1 it means the size wasnt defined for example 'Varies with device'
      if (numericSize == -1) {
        flag=1
        res = numericSize
      }
      // If it falls under this case, the value is in B, so we need to convert it to MB
      if(flag==0) {
        res = numericSize * 0.000001
      }

      res
    })

    // Rename 'Category' to 'Categories'
    var df_transformed = df_PS.withColumnRenamed("Category", "Categories")
    // Change 'Rating' datatype from String to double
    df_transformed = df_transformed.withColumn("Rating", col("Rating").cast("double"))
    // Change 'Rating' datatype from String to Long
    df_transformed = df_transformed.withColumn("Reviews", col("Reviews").cast("long"))
    // Apply size_transformation to 'Size', converting the values from string to double (value in MB), if size ins't defined it returns -1
    df_transformed = df_transformed.withColumn("Size", size_transformation(col("Size")))
    // Change 'Price' datatype from String to Double and present the value in euros
    df_transformed = df_transformed.withColumn("Price", col("Price").cast("double"))
    df_transformed = df_transformed.withColumn("Price", col("Price") * 0.9)
    // Rename 'Content Rating' to 'Content_Rating'
    df_transformed = df_transformed.withColumnRenamed("Content Rating", "Content_Rating")
    // Change 'Genre' datatype from String to Array[String]
    df_transformed= df_transformed.withColumn("Genres", split(col("Genres"), ";").cast("array<string>"))
    // Change 'Last Updated' datatype from String to Date and rename to 'Last_Updated'
    df_transformed = df_transformed.withColumn("Last Updated", to_date(col("Last Updated"), "MMMM d, yyyy"))
    df_transformed = df_transformed.withColumnRenamed("Last Updated", "Last_Updated")
    // Rename 'Current Ver' to 'Current_Version'
    df_transformed = df_transformed.withColumnRenamed("Current Ver", "Current_Version")
    // Rename 'Android Ver' to 'Minimum_Android_Version'
    df_transformed = df_transformed.withColumnRenamed("Android Ver", "Minimum_Android_Version")
    //df_transformed.printSchema()

    // For each 'App' get the record with more Reviews
    val maxReviews = df_transformed.groupBy("App").agg(max("Reviews").alias("MaxReviews"))

    // Group by 'App' and collect all unique categories into an Array[String]
    val joinedCategories = df_transformed.groupBy("App")
      .agg(collect_set("Categories").alias("JoinedCategories"))

    // Join the transformed DataFrame with maxReviews to get, for each 'App', the record with more Reviews
    var resultDF = df_transformed.join(maxReviews, Seq("App"))
      .filter(col("Reviews") === col("MaxReviews"))
      .drop("MaxReviews")
      .dropDuplicates("App")

    // Join to, for each 'App', substitute the 'Categories' with the values in 'JoinedCategories' (contains all Categories for each 'App')
    val finalDF = resultDF
      .join(joinedCategories, Seq("App"), "inner")
      .withColumn("Categories", col("JoinedCategories"))
      .drop("JoinedCategories")

    /* For testing purposes
    finalDF.show()
    finalDF.printSchema()
    val test1 = finalDF.filter(col("App") === "Clash of Clans")
    val test2 = finalDF.filter(col("App") === "Google Chrome: Fast & Secure")

    test1.show()
    test2.show()
     */
    finalDF
  }


  def part4(df_1: DataFrame, df_3: DataFrame): DataFrame = {

    // Joining df_3 and df_1 on column "App"
    val finalDF = df_3.join(df_1, Seq("App"))

    val directoryPath = "src/main/resources/datasets/Results/Part4/"

    // Save the final Dataframe as a parquet file with gzip compression
    finalDF.write
      .option("compression", "gzip")
      .mode("overwrite")
      .parquet(directoryPath)

    // Change parquet file name
    // Get a list of files in the directory
    val files = new File(directoryPath).listFiles()

    // Filter parquet files
    val parquetFiles = files.filter(_.getName.toLowerCase.endsWith(".parquet"))

    //
    val parquetFile = parquetFiles.head.toString

    Try(new File(parquetFile).renameTo(new File("src/main/resources/datasets/Results/Part4/googleplaystore_cleaned.parquet"))).getOrElse(false)

    finalDF

  }

  def part5(df_3: DataFrame): Unit = {

    // Change 'Rating' datatype from String to Double
    val df_4 = df_3.withColumn("Rating", col("Rating").cast("double"))

    // Explode the 'Genres' array so that each genre becomes a separate row
    val explodedDF = df_4.withColumn("Genre", explode(col("Genres")))

    // Group by 'Genres' and aggregate count, average rating, and average sentiment polarity
    val resultDF = explodedDF.groupBy("Genre")
      .agg(
        count("App").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )

    /* For testing Purposes

    resultDF.show()
    resultDF.printSchema()

    val test_1 = resultDF.filter(col("Genre") === "Art & Design")
    val test_2 = resultDF.filter(col("Genre") === "Pretend Play")
    val test_3 = resultDF.filter(col("Genre") === "Creativity")
    val test_4 = resultDF.filter(col("Genre") === "Strategy")
    test_1.show()
    test_2.show()
    test_3.show()
    test_4.show()
    println(resultDF.count())
    */

    val directoryPath = "src/main/resources/datasets/Results/Part5/"

    // Save the final Dataframe as a parquet file with gzip compression
    resultDF.write
      .option("compression", "gzip")
      .mode("overwrite")
      .parquet(directoryPath)

    // Change parquet file name
    // Get a list of files in the directory
    val files = new File(directoryPath).listFiles()

    // Filter parquet files
    val parquetFiles = files.filter(_.getName.toLowerCase.endsWith(".parquet"))

    val parquetFile = parquetFiles.head.toString

    Try(new File(parquetFile).renameTo(new File("src/main/resources/datasets/Results/Part5/googleplaystore_metrics.parquet"))).getOrElse(false)
  }


  def main(args : Array[String]) {
    try {
      // Set HADOOP_HOME for Windows environment
      val file = new File("src/main/resources/HadoopResources")
      System.setProperty("hadoop.home.dir", file.getAbsolutePath)

      // Create SparkSession
      val spark = SparkSession.builder()
        .appName("app")
        .master("local[*]")
        .getOrCreate()

      // Read CSV file
      val df_UR = spark.read
        .option("header", true)
        .option("sep", ",")
        .option("quote", "\"")
        .option("escape", "\"")
        .csv("src/main/resources/datasets/googleplaystore_user_reviews.csv")

      val df_PS = spark.read
        .option("header", true)
        .option("sep", ",")
        .option("quote", "\"")
        .option("escape", "\"")
        .csv("src/main/resources/datasets/googleplaystore.csv")

      //--------------------------------- Part1  ---------------------------------
      val df_1 = part1(df_UR)
      println("--------------------------------- Part1 Result ---------------------------------")
      df_1.printSchema()
      df_1.show()
      println("--------------------------------------------------------------------------------")


      //--------------------------------- Part2  ---------------------------------
      part2(df_PS)

      val df_res_part2 = spark.read
        .option("header", true)
        .option("sep", "§")
        .option("quote", "\"")
        .option("escape", "\"")
        .csv("src/main/resources/datasets/Results/Part2/best_apps.csv")

      println("--------------------------------- Part2 Result ---------------------------------")
      df_res_part2.printSchema()
      df_res_part2.show()
      println("--------------------------------------------------------------------------------")


     //--------------------------------- Part3  ---------------------------------
     println("--------------------------------- Part3 Result ---------------------------------")
     val df_3 = part3(df_PS)
      df_3.printSchema()
      df_3.show()
      println("--------------------------------------------------------------------------------")


      //--------------------------------- Part4  ---------------------------------
      val df_4 = part4(df_1, df_3)

      val df_res_part4 = spark.read
        .option("header", "true")
        .parquet("src/main/resources/datasets/Results/Part4/googleplaystore_cleaned.parquet")

      println("--------------------------------- Part4 Result ---------------------------------")
      df_res_part4.printSchema()
      df_res_part4.show()
      println("--------------------------------------------------------------------------------")

      //--------------------------------- Part5  ---------------------------------

      // DISCUSSION: I think it needs to be df_4 instead of df_3, because df_3 doesn´t have 'Average_Sentiment_Polarity' column
      part5(df_4)

      val df_res_part5 = spark.read
        .option("header", "true")
        .parquet("src/main/resources/datasets/Results/Part5/googleplaystore_metrics.parquet")
      println("--------------------------------- Part5 Result ---------------------------------")
      df_res_part5.printSchema()
      df_res_part5.show()
      println("--------------------------------------------------------------------------------")

      // Stop SparkSession
      spark.stop()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
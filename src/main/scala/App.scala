import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object App {

  def import_data(spark : SparkSession, sep: String, filename: String) :DataFrame= {
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep",sep)
      .load(filename)
    return df
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Zohir's spark app").master("local[*]").getOrCreate()

    val df_insee = import_data(spark, ";", "esgi_tp/code-insee-postaux-geoflar.csv")//.persist()
    val df_insee2 = df_insee.select("CODE INSEE", "Code Dept", "geom_x_y")
    println(df_insee2.show(3))

    val df_communes = import_data(spark, ";", "esgi_tp/Communes.csv")
    val df_communes2 = df_communes.select("DEPCOM", "PTOT")
    println(df_communes2.show(3))

    val df_postes = import_data(spark, ";", "esgi_tp/postesSynop.txt")
    val df_postes2 = df_postes.select("ID", "Latitude", "Longitude")
    println(df_postes2.show(3))

    val df_synop = import_data(spark, ";", "esgi_tp/synop.2020120512.txt")
    val df_synop2 = df_synop.select("date", "numer_sta", "t")
    println(df_synop2.show(3))

    // Le rapport doit contenir l'intégralité des départements français et des stations Météo-France.
    // Si deux stations sont dans le même département, les infos de population doivent être dupliquées.
    // Si un département ne contient pas de station météorologique, des valeurs Null doivent apparaître.
    // Si une station n'est pas en France, elle doit être ignorée.
    // On rattachera les stations météo à la commune la plus proche géographiquement en utilisant les latitude et longitude pour déterminer son département

    // ajout de la variable population de de departement
    val df1 = df_insee2.join(df_communes2, df_insee2("CODE INSEE") === df_communes2("DEPCOM"), "left_outer")
    println(df1.show(5))
    // ajout de la variable temperature ( jointure full pour recuperer aussi les ville sans temp)
    val get_lat : String => String = _.split(",")(0)
    val extract_get_lat = udf(get_lat)
    val df2 = df1.withColumn("lat", extract_get_lat(col("geom_x_y").cast(DoubleType)))
    val get_long : String => String = _.split(",")(1)
    val extract_get_long = udf(get_long)
    val df3 = df2.withColumn("long", extract_get_long(col("geom_x_y").cast(DoubleType)))

    println("df3 :")
    println(df3.printSchema())
    println(df_postes2.printSchema())
    println(df3.show(5))
    val df4 = df3.join(df_synop2, df3("lat") === df_postes2("Latitude") && df3("long") === df_postes2("Longitude"), "left_outer")
    println(df4.show(5))
    val df5 = df4.join(df_postes2, df4("numer_sta") === df_postes2("ID"), "full")
    println(df5.show(5))

    // Pour la Conversion K °C il est demandé de réaliser une UDF (t°C = K - 273,15) (1 pts)
    val temp_celsius : Double => Double = _ - 273.15
    val extract_temp_celsius = udf(temp_celsius)
    val df6 = df5.withColumn("temp_celsius", extract_temp_celsius(col("t")))

    println("Done!")

  }


}

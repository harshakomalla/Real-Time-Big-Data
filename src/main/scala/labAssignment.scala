import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LabAssign {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkTransformation").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val textfile = sc.textFile("inputdata.csv")

    val rows = textfile.map(line => line.split(","))

    val distinct=  rows.map(row => row(2)).distinct.count
    println( "  The distinct value count is " + distinct)

    val davidRows = rows.filter(row => row(1).contains("DAVID"))
    val value = davidRows.count

    println( " The number of rows with David value are " + value)

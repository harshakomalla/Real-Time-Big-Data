
import org.apache.spark.{SparkContext, SparkConf}


object Assignment_One  {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","F:\\winutils");

    val sparkConf = new SparkConf().setAppName("Assignment_One").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val input=sc.textFile("input")

    val wc=input.map(line=>(line,1))

    val outpt=wc.reduceByKey(_+_)

    val output=outpt.coalesce(1)

    output.saveAsTextFile("output")

    val o=input.map(line=>line).count()

    val sort=output.sortByKey()

    val sorted=outpt.coalesce(1)

    sorted.saveAsTextFile("output")

    //var s:String="Lines:Count \n"
    // o.foreach{case(line,count)=>{

    // s+=line+" : "+count+"\n"
    println(output)
    println(sorted)

  }
}

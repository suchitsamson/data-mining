import java.io.{BufferedWriter, File, FileWriter}
import org.apache.spark.sql.SparkSession


object suchit_mehta_task2 {

  def main(args:Array[String]): Unit ={
    val start = System.nanoTime()
    val inputFile1 = args(0)
    val inputFile2 = args(1)
    val outputFile1 = args(2)
    val outputFile2 = args(3)

    val ss = SparkSession.builder().appName("suchit_mehta_task1")
      .config("spark.master","local[*]").getOrCreate()

    val reviewRDD = ss.read.json(inputFile1).rdd
    val businessRDD = ss.read.json(inputFile2).rdd

    val statesRDD = businessRDD.map(x=>(x.getAs[String]("business_id"),x.getAs[String]("state")))
    val starsRDD = reviewRDD.map(x=>(x.getAs[String]("business_id"),x.getAs[Double]("stars")))
    val stateAndStarsRDD = statesRDD.join(starsRDD)
    val avgStarsPerStateOrder = stateAndStarsRDD.map(x=> x._2).groupByKey().mapValues(x=> x.sum / x.size ).sortBy(x=>(-x._2,x._1))

    val m1_start = System.nanoTime()
    val avgStarsPerStateOrderList = avgStarsPerStateOrder.collect()
    for(i<- 0 to 4){
      println(avgStarsPerStateOrderList(i))
    }
    val m1_end = System.nanoTime()

    var outputString1 = "state,stars\n"
    for(i<- 0 to avgStarsPerStateOrderList.size-1){
      outputString1+=avgStarsPerStateOrderList(i)._1+","+avgStarsPerStateOrderList(i)._2+"\n"
    }
    val file = new File(outputFile1)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(outputString1)
    bw.close()


    val m2_start = System.nanoTime()
    val avgStarsPerStateOrderList_m2 = avgStarsPerStateOrder.take(5)
    for(i<- 0 to 4){
      println(avgStarsPerStateOrderList_m2(i))
    }
    val m2_end = System.nanoTime()

    var outputString2 = "{\n\t\"m1\": "+(m1_end-m1_start)+",\n\t\"m2\": "+(m2_end-m2_start)+",\n\t\"explanation\": \"m1>m2 because for m1 additional time is required to collect all the items while for m2 only mandatory(5 items) are collected. Hence time is saved.\"\n}"
    val file2 = new File(outputFile2)
    val bw2 = new BufferedWriter(new FileWriter(file2))
    bw2.write(outputString2)
    bw2.close()

    println(System.nanoTime()-start)
  }
}

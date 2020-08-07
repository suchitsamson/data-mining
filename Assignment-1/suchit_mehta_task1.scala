import org.apache.spark.sql.SparkSession
import java.io._

object suchit_mehta_task1 {
  def main(args:Array[String]): Unit ={
    val start = System.nanoTime()
    val inputFile = args(0)
    val outputFile = args(1)
    val ss = SparkSession.builder().appName("suchit_mehta_task1")
      .config("spark.master","local[*]").getOrCreate()
    val jsonRDD = ss.read.json(inputFile).rdd

    val usefulCount = jsonRDD.map(x=>{val xyz = if(x.getAs[Long]("useful")>0)1 else 0
      xyz}).reduce((a,b)=>a+b)
    val fiveStarRating = jsonRDD.map(x=>{val xyz = if(x.getAs[Double]("stars")==5)1 else 0
      xyz}).reduce((a,b)=>a+b)

    val longestReview = jsonRDD.map(x=>x.getAs[String]("text").length()).max()
    val userReviewRDD = jsonRDD.map(x=>(x.getAs[String]("user_id"),1)).reduceByKey((a,b)=>a+b).sortBy(c=>(-c._2,c._1))
    val top20userReviewsList = userReviewRDD.take(20)
    val businessReviewsRDD = jsonRDD.map(x=>(x.getAs[String]("business_id"),1)).reduceByKey((a,b)=>a+b).sortBy(c=>(-c._2,c._1))
    val top20businessReviewsList = businessReviewsRDD.take(20)

    var userList="["
    for(i<- 0 to top20userReviewsList.size){
      userList+="[\""+top20userReviewsList(i)._1+"\", "+top20userReviewsList(i)._2+"],"
    }

    val finalUserList = userList.substring(0,userList.length-1)+"]"
    var businessList="["
    for(i<- 0 to top20businessReviewsList.size){
      businessList+="[\""+top20businessReviewsList(i)._1+"\", "+top20businessReviewsList(i)._2+"],"
    }

    val finalBusinessList = businessList.substring(0,businessList.length-1)+"]"
    val outputString = "{\n"+"\t\"n_review_useful\": "+usefulCount+",\n\t\"n_review_5_star\": "+fiveStarRating+",\n\t\"n_characters\": "+longestReview+",\n\t\"n_user\": "+userReviewRDD.count()+",\n\t\"top20_user\": "+finalUserList+",\n\t\"n_business\": "+businessReviewsRDD.count()+",\n\t\"top20_business\": "+finalBusinessList+"\n}"

    val file = new File(outputFile)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(outputString)
    bw.close()
    println(System.nanoTime()-start)
  }
}

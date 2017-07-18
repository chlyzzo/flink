package org.apache.flink.ml.preprocessing

import org.apache.flink.api.scala._
import scala.math.log
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.math.VectorBuilder
import breeze.linalg.{DenseVector => BDV,SparseVector =>BSV}
/**
it is the tf-idf compute method,can get the documents tf value and idf value and tf-idf value,and can get vector.

the example:
  val data = ....//DataSet[(String,Array[String]),document id and the document all words,
  val tfidfValue = computeTFIDFValue(data)
  val tfidfVec = computeTFIDFToVector(data)
  val allWords = computeWordsDictionary(tfidfValue)
....
and then can use the vector compute cos similarty.

**/
object TfIdf {

  /**
   compute docs tf-idf value,return the pre-vector,it can use to transfer the SparseVector.

   **/
  def computeTFIDFValue(tfidfs:DataSet[(String,Array[(String,Double)])]):DataSet[(String,Int,Array[(Int, Double)])]={
      val allWords = computeWordsDictionary(tfidfs)

      val TFIDF_MODEL = tfidfs.map(new RichMapFunction[(String,Array[(String,Double)]),(String,Int,Array[(Int, Double)])](){
          import scala.collection.JavaConverters._
          var allWordsBroad:scala.collection.mutable.Buffer[String]= null

          override def open(config: Configuration): Unit = {
              allWordsBroad = getRuntimeContext().getBroadcastVariable[String]("allwords").asScala
          }

          override def map(value:(String,Array[(String,Double)])):(String,Int,Array[(Int, Double)])={
             val docId = value._1
             val idvalueMap = value._2.map{ x=>
                val i = allWordsBroad.indexOf(x._1)
                (i,x._2)
             }.sortBy(_._1)
             (docId,allWordsBroad.size,idvalueMap.toArray)
          }

    }).withBroadcastSet(allWords, "allwords")

    TFIDF_MODEL
  }

  /**
   compute docs tf-idf value,return the vector,
   DataSet[(String,Vector)],document id,and the tf-idf vector.
   **/
  def computeTFIDFToVector(tfidfs:DataSet[(String,Array[(String,Double)])]):DataSet[(String,Vector)]={
      val allWords = computeWordsDictionary(tfidfs)

      val TFIDF_MODEL = tfidfs.map(new RichMapFunction[(String,Array[(String,Double)]),(String,Vector)](){
          import scala.collection.JavaConverters._
          var allWordsBroad:scala.collection.mutable.Buffer[String]= null

          override def open(config: Configuration): Unit = {
              allWordsBroad = getRuntimeContext().getBroadcastVariable[String]("allwords").asScala
          }

          override def map(value:(String,Array[(String,Double)])):(String,Vector)={
             val docId = value._1
             val idvalueMap = value._2.map{ x=>
                val i = allWordsBroad.indexOf(x._1)
                (i,x._2)
             }.sortBy(_._1)

             val bsv1 = new BSV[Double](idvalueMap.map(_._1),idvalueMap.map(_._2),allWordsBroad.size)
             (docId,VectorBuilder.sparseVectorBuilder.build(bsv1.toArray.toList))
          }

    }).withBroadcastSet(allWords, "allwords")

    TFIDF_MODEL
  }

  /**
    get all dataset Dictionary
    **/
  def computeWordsDictionary(data:DataSet[(String,Array[(String,Double)])]):DataSet[String]={
       data.flatMap{doc =>doc._2.map(_._1)}.distinct().name("get vocab")
  }

  /**
   compute some documents tf value,
   **/
  def computeTF(data:DataSet[(String, Array[String])]):DataSet[(String,Array[(String, Int)])]={
     data.map{ m =>
          val re = m._2.groupBy { w => w}.map{e =>(e._1,e._2.length)}.toArray
         (m._1, re)
     }
  }

  /**
  compute one document tf value,
  **/
  def computeTF(data:(String, Array[String])):List[(String, String, Int)]={
      val id = data._1
      val result = data._2.groupBy { w => w}.map{ e =>
         (id,e._1,e._2.length)
       }
       result.toList
  }

   /**
    compute some documents idf value
    **/
  def computeIDF(data:DataSet[(String, Array[String])]):DataSet[(String, Int)]={
     data.flatMap{m=>m._2.toSet}
     .map { m => (m,1) }
     .groupBy(0)
     .sum(1)
  }




  /**
   compute dataset tf-idf value,use the one by one compute,
   **/
  def computeTFIDFOneByOne(data:DataSet[(String, Array[String])]):DataSet[(String,Array[(String,Double)])]={

         import scala.collection.JavaConverters._
         import breeze.linalg._

         val dataBroad = data.map{x=>x}.name("broad cast")

         val result = data.map(new RichMapFunction[(String, Array[String]),(String,Array[(String,Double)])](){

             var toDataBroad:scala.collection.mutable.Buffer[(String, Array[String])]= null

             override def open(config: Configuration): Unit = {
                     toDataBroad = getRuntimeContext().getBroadcastVariable[(String, Array[String])]("toData").asScala
             }

            override def map(from:(String, Array[String])):(String,Array[(String,Double)])={
                     val docWordCount = from._2.groupBy {w => w}.map{ e =>(e._1,e._2.length)}
                     val total = toDataBroad.length.toDouble

                     val docId = from._1
                     val words = from._2.toSet
                     val res = toDataBroad.filter(_._1 != docId)
                     val returnData = words.map { word =>
                         var countIDF = 1.0
                         res.foreach{otherDoc=>
                            if (otherDoc._2.toSet.contains(word)){
                              countIDF = countIDF + 1.0
                            }
                         }
                         val tdidf = docWordCount.get(word).get*Math.log((total+1.0)/(countIDF+1.0))
                         (word,tdidf)
                     }
                     (docId,returnData.toArray)
          }
      }).withBroadcastSet(dataBroad, "toData")
      result
  }
}

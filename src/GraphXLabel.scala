/**
  * Created by chenweigen on 2016/7/13.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//参考:http://www.cnblogs.com/shishanyuan/p/4747793.html
object GraphXLabel {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("GraphXLabel").setMaster("local")
    val sc = new SparkContext(conf)
    //设置顶点和边，注意顶点和边都是用元组定义的Array
    //顶点的数据类型是VD:(String,Int)
    val vertexArray = Array(
      (1L, (1.0,0.0)),
      (2L, (1.0,0.0)),
      (3L, (0.0, 0.0))
    )
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(1L, 2L, 0.3),
      Edge(1L, 3L, 0.7),
      Edge(2L, 3L, 0.5)
    )
    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (Double, Double))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edgeArray)
    //构造图Graph[VD,ED]
    val graph: Graph[(Double, Double), Double] = Graph(vertexRDD, edgeRDD)
//    graph.edges.filter(ed=>ed.srcId==1).collect.foreach(ed=>println(ed.srcId+" to "+ed.dstId))
    val initialGraph = graph.mapVertices((a,b)=>b)
    var step=1
    val ssp=initialGraph.pregel(0.0,1)(
      (vt,labela,labelb)=>{
        println("step"+step+"----"+"vt:"+vt+","+"labela:"+labela.toString()+",labelb:"+labelb)
        step=step+1
        (labelb,labela._2)
      } ,
      triplet => {
        println("step"+step+"----"+"srcId:"+triplet.srcId+",dstAttr:"+triplet.dstAttr._1+",attr:"+triplet.attr)
//        println("triplet.dstAttr._1:"+triplet.dstAttr._1)
//        println("triplet.attr:"+triplet.attr)
        step=step+1
        Iterator((triplet.srcId,triplet.dstAttr._1*triplet.attr))
      },
      (a,b) =>{
        println("step"+step+"----"+"a:"+a+",b:"+b)
        step=step+1
        println("=======")
        a+b
      }
    )
    initialGraph
    println(ssp.vertices.collect.mkString("\n"))
    sc.stop()
  }
}

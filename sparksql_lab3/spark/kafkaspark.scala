import java.util.HashMap
import scala.util.Random

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import kafka.serializer.{DefaultDecoder, StringDecoder}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

object KafkaWordCount {
  def main(args: Array[String]) {

    val kafkaConf = Map(
	"metadata.broker.list" -> "localhost:9092",
	"zookeeper.connect" -> "localhost:2181",
	"group.id" -> "kafka-spark-streaming",
	"zookeeper.connection.timeout.ms" -> "1000")

    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Set("avg"))
    
// it recieves messages having ("avg" :String ,[letter:String , number:Int]
    val values = messages.map( _._2)

// pairs=([letter:String , number:double],1)
    
    val pairs =values.flatMap{ _.split(" ") }.map (i => { val s=i. split(",") 
							(s(0),s(1).toInt)                                                                                 
                                                         }
						  )

	
        
// it recieve a tuple as input and split that tuple to key ,value and returns  key -> avg

def mappingFun(key: String, value: Option[Int], state: State[(Double,Long)] )  : Option[(String,Double)] =  {
                                                
                       
                       val (prevSum,prevN)= state.getOption.getOrElse((0D,0L))
                       val (sum,n) = ((prevSum +value.getOrElse(0) ).toDouble , prevN +1L)
                                           
                       val output = (key, sum/n)
                       state.update((sum,n))
                       Some(output)
    }
    
	
    val stateSpec = StateSpec.function(mappingFun _)
    val stateDstream = pairs.mapWithState(stateSpec)
    println(" this is the output  ..........")
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

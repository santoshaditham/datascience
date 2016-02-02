package DefectTracker.EclipseMozilla

/**
 * @author ${user.name}
 */

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._
import org.apache.spark.mllib.clustering.{LDA, KMeans, KMeansModel, GaussianMixture, GaussianMixtureModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.mllib.classification.NaiveBayes
import kafka.producer.SyncProducerConfig;
import org.apache.spark.streaming.kafka.{ HasOffsetRanges, KafkaUtils }
import org.apache.spark.streaming.{ Minutes, Seconds, StreamingContext }
import collection.mutable.HashMap
import collection.mutable.Set
import collection.immutable.Map
import java.security.MessageDigest
import scala.collection.immutable.ListMap
import scala.util.control.Breaks.{break, breakable}
import scala.io.Source
import scala.util.parsing.json._
import kafka.serializer.{ DefaultDecoder, StringDecoder }
import kafka.javaapi.producer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;


object App {
  
  /* for unique representation to word/string */
  def md5(s: String) = {
    val base = MessageDigest.getInstance("MD5").digest(s.getBytes).map("%02X".format(_)).mkString
    base.toInt
  }
  
  /* dot product between two vectors */
  def dot(x: Array[Double], y: Array[Double]) : Double = {
    var dotProduct = 0.0
    if(x.size == y.size){ dotProduct = (x zip y).map{Function.tupled(_ * _)}.sum }
    return dotProduct
  }
  
  /* squared root of sum of powers */
  def sqrtOfPow(x: Array[Double], y: Array[Double]) : Double = {
    var sqrtValue = 0.0
    if(x.size == y.size){
      val xySquared = (x zip y).map { xy => scala.math.pow(xy._1, 2) + scala.math.pow(xy._2, 2) }.sum
      sqrtValue = scala.math.sqrt(xySquared)
    }
    return sqrtValue 
  }
  
  /* template lines have format = <required> [bunch of keys words] <optional>
   * source lines have format = [bunch of words: technical and non-technical]
   * so we find longest common substring and transform source to look like template
   * and then find cosine similarity
  */
  def cosineSim(sourceLine: String, templateLine: String): Double = {
    var similarity = 0.0
    val templateLineVector = templateLine.split(" ").map(md5(_).toDouble)
    val sourceLineVector = sourceLine.split(" ").map(md5(_).toDouble)
    if(sourceLineVector.length > 0){
      val numerator = dot(sourceLineVector, templateLineVector)
      val denominator = sqrtOfPow(sourceLineVector, templateLineVector)
      similarity = numerator/denominator
    }
    return similarity
  }
 
  
  def main(args : Array[String]) {

    // Configs
    val master = "spark://quickstart.cloudera:7077"
    val sparkConf = new SparkConf()
    .set("spark.storage.memoryFraction", ".8")
    .set("spark.executor.memory", "2g")
    .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
    .set("spark.sql.inMemoryColumnarStorage.batchSize", "10000")

    // Contexts    
    val sc = new SparkContext("local[2]","Spark-MLlib",sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)  
    
    /* 
     * Connect to json and get bug data from Eclipse and Mozilla Defect Tracking Dataset. Each bug has 12 attributes 
     [Reports; Assigned to; CC; Status; Priority; Severity; Product; Component; OS; Version; Description; Resolution]
     classified into 2 classes:
     report - opening, reporter, current status, current resolution
     general - when, what, who
     * 
     */
 
    case class report(opening:Long, reporter: String, current_status: String, current_resolution: String)
    var fileUrl = """https://github.com/ansymo/msr2013-bug_dataset/tree/master/data/v02/eclipse/reports.json"""
    var df = sqlContext.read.json(fileUrl)
    val e_reports = df.map(_.mkString("").split(",")).map(p => (p(0).toInt, report(p(1).toLong, p(2), p(3), p(4))))

    case class general(when: Long, what: String, who: String)
    fileUrl = """https://github.com/ansymo/msr2013-bug_dataset/tree/master/data/v02/eclipse/assigned_to.json"""
    df = sqlContext.read.json(fileUrl)
    val e_assigned_to = df.map(_.mkString("").split(",")).map(p => (p(0).toInt, general(p(1).toLong, p(2), p(3))))
    
    fileUrl = """https://github.com/ansymo/msr2013-bug_dataset/tree/master/data/v02/eclipse/cc.json"""
    df = sqlContext.read.json(fileUrl)
    val e_cc = df.map(_.mkString("").split(",")).map(p => (p(0).toInt, general(p(1).toLong, p(2), p(3))))
 
    fileUrl = """https://github.com/ansymo/msr2013-bug_dataset/tree/master/data/v02/eclipse/bug_status.json"""
    df = sqlContext.read.json(fileUrl)
    val e_bug_status = df.map(_.mkString("").split(",")).map(p => (p(0).toInt, general(p(1).toLong, p(2), p(3))))
  
    fileUrl = """https://github.com/ansymo/msr2013-bug_dataset/tree/master/data/v02/eclipse/priority.json"""
    df = sqlContext.read.json(fileUrl)
    val e_priority = df.map(_.mkString("").split(",")).map(p => (p(0).toInt, general(p(1).toLong, p(2), p(3))))
 
    fileUrl = """https://github.com/ansymo/msr2013-bug_dataset/tree/master/data/v02/eclipse/severity.json"""
    df = sqlContext.read.json(fileUrl)
    val e_severity = df.map(_.mkString("").split(",")).map(p => (p(0).toInt, general(p(1).toLong, p(2), p(3))))
 
    fileUrl = """https://github.com/ansymo/msr2013-bug_dataset/tree/master/data/v02/eclipse/product.json"""
    df = sqlContext.read.json(fileUrl)
    val e_product = df.map(_.mkString("").split(",")).map(p => (p(0).toInt, general(p(1).toLong, p(2), p(3))))

    fileUrl = """https://github.com/ansymo/msr2013-bug_dataset/tree/master/data/v02/eclipse/component.json"""
    df = sqlContext.read.json(fileUrl)
    val e_component = df.map(_.mkString("").split(",")).map(p => (p(0).toInt, general(p(1).toLong, p(2), p(3))))
  
    fileUrl = """https://github.com/ansymo/msr2013-bug_dataset/tree/master/data/v02/eclipse/op_sys.json"""
    df = sqlContext.read.json(fileUrl)
    val e_op_sys = df.map(_.mkString("").split(",")).map(p => (p(0).toInt, general(p(1).toLong, p(2), p(3))))

    fileUrl = """https://github.com/ansymo/msr2013-bug_dataset/tree/master/data/v02/eclipse/version.json"""
    df = sqlContext.read.json(fileUrl)
    val e_version = df.map(_.mkString("").split(",")).map(p => (p(0).toInt, general(p(1).toLong, p(2), p(3))))
   
    fileUrl = """https://github.com/ansymo/msr2013-bug_dataset/tree/master/data/v02/eclipse/short_desc.json"""
    df = sqlContext.read.json(fileUrl)
    val e_short_desc = df.map(_.mkString("").split(",")).map(p => (p(0).toInt, general(p(1).toLong, p(2), p(3))))
 
    fileUrl = """https://github.com/ansymo/msr2013-bug_dataset/tree/master/data/v02/eclipse/resolution.json"""
    df = sqlContext.read.json(fileUrl)
    val e_resolution = df.map(_.mkString("").split(",")).map(p => (p(0).toInt, general(p(1).toLong, p(2), p(3))))
     
    /*
     * 
    val mozillaData = fileNames.map { fileName => 
      val fileUrl = """https://github.com/ansymo/msr2013-bug_dataset/tree/master/data/v02/mozilla/""" + fileName + """.json"""
      val file = Source.fromURL(fileUrl).mkString
      val json:Option[Any] = JSON.parseFull(file) 
      val map:Map[String, Any] = json.get.asInstanceOf[Map[String, Any]]
      //val data:List[Any] = map.get("reports").get.asInstanceOf[List[Any]]
      (fileName , map)
      }.toMap
      * 
      */
      
    // Regex set
    val ignore = """,;)(}{+#*][|!`="?"""
    val separators = Array[Char](' ', '\n', '\t', '\r')
    val phonePattern = """([0-9]{1,3})[ -.]([0-9]{1,3})[ -.]([0-9]{4,10})""".r
    val datePattern = """([1-9]|0[1-9]|1[012])[- /]([1-9]|0[1-9]|[12][0-9]|3[01])[- /.](\d{2}|\d{4})""".r
    val timePattern = """(0[1-9]|[12][0-9]):(\d{2}):(\d{2})""".r
    val shortTimePattern = """(\d{1}|\d{2}):(\d{2})""".r
    val longTimePattern = """(0[1-9]|[12][0-9]):(\d{2}):(\d{2}).(\d{3})""".r
    val emailPattern = """(\w+)@([\w\.]+)""".r
    val emailPattern1 = """(^[a-zA-Z0-9_-]*$)@([\w\.]+)""".r
    val linkPattern = """(http|https|ftp)://(\w+)""".r
    val numberPattern = """(\d{10})""".r
    val bugPattern = """(\d{4})-(\d{4})-(\d{4})""".r
    val tagPattern = """(<(.|\n)+?>)""".r
    val tagPattern1 = """(<^[a-zA-Z0-9_-]*$>)""".r
    val randomPattern = """([.]{3,}|[>]{3,}|[-]{3,})""".r
    val wordPattern1 = """\w*ed\b""".r
    val wordPattern2 = """\w*ly\b""".r
    val wordPattern3 = """\w*ing\b""".r
    
    // get info stored in all help files - dictionary words, stop words, names etc
    val filebufDictionary = Source.fromFile("/usr/share/dict/words")
    val filebufStopWords = Source.fromFile("stopwordsFile.txt")
    val englishDictionary: scala.collection.Set[String] = filebufDictionary.getLines().toSet
    val stopwords = filebufStopWords.getLines().toList
    filebufDictionary.close()
    filebufStopWords.close()

    
    /* 
     * Get bug descriptions & resolutions, clean it and parse it into an array of words. 
     * Do for all old bugs. Call this as CORPUS 
     */ 
    
    val data = e_reports.filter(_._2.current_status=="RESOLVED")
      .map { r =>
        val value = (e_short_desc.lookup(r._1).head.what + " " + e_resolution.lookup(r._1).head.what).split(" ")
        (r._1, value)
      }
    val corpus = data.map(row => {
        val thisBugID = row._1
        val thisBug = row._2
        var thisHistory:String = Option(thisBug.toString()).getOrElse("no history")
        val splits = thisHistory.toLowerCase()
          .filterNot { ch => ignore.indexOf(ch) >= 0 }
          .split(separators)
        val words = splits.map { w => w
          .stripSuffix(".").stripSuffix(":").stripSuffix(",").stripSuffix("~").stripSuffix("-")
          .stripPrefix(".").stripPrefix(":").stripPrefix(",").stripPrefix("~").stripPrefix("-")}
        val cleanWords = words
          .filterNot { w => stopwords.contains(w) } //stop words
          //.filterNot { w => englishDictionary.contains(w) } 
          .filterNot { w => w.length()==1 } //single chars
          .filterNot { w => bugPattern.unapplySeq(w).isDefined}
          .filterNot { w => emailPattern.unapplySeq(w).isDefined}
          .filterNot { w => emailPattern1.unapplySeq(w).isDefined}
          .filterNot { w => phonePattern.unapplySeq(w).isDefined}
          .filterNot { w => numberPattern.unapplySeq(w).isDefined}
          .filterNot { w => datePattern.unapplySeq(w).isDefined}
          .filterNot { w => timePattern.unapplySeq(w).isDefined}
          .filterNot { w => shortTimePattern.unapplySeq(w).isDefined}
          .filterNot { w => longTimePattern.unapplySeq(w).isDefined}
          .filterNot { w => randomPattern.unapplySeq(w).isDefined}
          .filterNot { w => bugPattern.unapplySeq(w).isDefined}
          .filterNot { w => tagPattern.unapplySeq(w).isDefined}
          .filterNot { w => tagPattern1.unapplySeq(w).isDefined}
          .filterNot { w => wordPattern1.unapplySeq(w).isDefined}
          .filterNot { w => wordPattern2.unapplySeq(w).isDefined}
          .filterNot { w => wordPattern3.unapplySeq(w).isDefined}
          .filterNot { w => 
            val z: Seq[Char] = w 
            z match {
              case Seq('h','t','t','p', rest @ _*) => true
              case Seq('f','t','p', rest @ _*) => true
              case Seq(_*) => false}}
        (thisBugID, cleanWords)
      })
    corpus.cache()  
    val vocab_counts = corpus.map(_._2)
      .flatMap { x => x.toSeq }
      .groupBy(identity)
      .map{ case(a,b) => (a,b.size)}
    vocab_counts.cache()
    val vocabulary = vocab_counts.map(_._1).zipWithIndex().map(v => (v._1, v._2.toInt))
    vocabulary.cache()
    val vocabularyInverse = vocabulary.map(_.swap)
    vocabularyInverse.cache()
    println("Complete: parsed data into corpus -- "+corpus.count())
    
    
    // Prepare context-aware documents from CORPUS 
    val documents = corpus.map { case (id, doc) =>
      val bagOfWords = doc.toSet
      val numericTokens = bagOfWords.map{w =>
        var (countWord, longWord, problemWordScore, solutionWordScore) = (0.0, 0.0,0.0,0.0)  
        //RULE1: word count > 3 means more meaning
        if(doc.count(_.equals(w)) > 3) 
          countWord = 10.0
        //RULE2: word length > 7 means more meaning
        if(w.length()>7) 
          longWord = 100.0
        //RULE3: word indicates problem description
        if(e_short_desc.lookup(id).head.what.split(" ").contains(w))
          problemWordScore = 1000.0
        //RULE4: word indicates solution description
        if(e_resolution.lookup(id).head.what.split(" ").contains(w))
          solutionWordScore = 10000.0
        //VALUE for term in [0, 11110]
        val score = (countWord + longWord + problemWordScore + solutionWordScore).toDouble
        (vocabulary.lookup(w).head, score)
        }
      val vectorInput = numericTokens.toMap
      val indices = vectorInput.map(_._1.toInt).toArray
      val values = vectorInput.map(_._2).toArray
      val docInput = Vectors.sparse(vocabulary.count().toInt, indices, values)
      (id.toLong, docInput)
    }
    documents.cache()
    println("Complete: integer values to documents")
    
    // Cluster the documents into topics using LDA (symmetric priors)
    val numOfTopics = 5
    val alpha = 50/numOfTopics + 1
    val beta = (200/vocabulary.count()) + 1
    println(s"alpha is $alpha and beta is $beta")
    val ldaModel = new LDA()
      .setK(numOfTopics)
      .setMaxIterations(30)
      .setDocConcentration(alpha) //high
      .setTopicConcentration(beta) //low
      .run(documents)
    println("Complete: LDA")    
    
    // Interpret results from LDA
    val topicIndices = ldaModel.describeTopics(ldaModel.vocabSize/numOfTopics) //top 20%
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => 
        (vocabularyInverse.lookup(term).head, weight) }
    }
    
    /* Give technicality for documents using LDA topic classification. */
    val docsAsHighValueTerms = corpus.map { case (id, doc) =>
      val hivalTerms = doc.filter(term => topics.contains(term))
      (id, hivalTerms)
      } 
    docsAsHighValueTerms.cache()
    println("Complete: technical value for docs")
    
    
    // Cluster the frequent patterns of important words to gain more knowledge from history
    val minSup = 0.25 //very low as items are unique
    val fpg = new FPGrowth().setMinSupport(minSup) 
    val patterns = fpg.run(corpus.map(_._2.toSet.toArray))
    println("Complete: FP growth")

    // Create 2-dimension data [what we know, what we guessed]
    // for future, change this to more dimensions. Else results will be biased
    val fpWords = patterns.freqItemsets.flatMap(_.items).collect()
    val docsAsFPTerms = corpus.map { case (id, doc) =>
      val fpTerms = doc.filter(word => fpWords contains word)
      (id, fpTerms)
      } 
    docsAsFPTerms.cache()
    println("Complete: getting description data into technical dimension")
    
    // for each word in a case check - lda topic, fp, priority, severity
    val computedValues = corpus.map { case (id, doc) =>
        var (severityScore, priorityScore) = (0.0,0.0)  
        //RULE: if priority of report is
        val priorities = List("P1", "P2", "P3")
        if(priorities.contains(e_priority.lookup(id).head.what)) 
          severityScore = 100.0
        //RULE: if severity of report is  
        val severities = List("major", "critical", "enhancement")
        if(severities.contains(e_severity.lookup(id).head.what))  
          severityScore = 100.0
        val updatedNumericTokens = doc.map{ w =>
          var (ldaWordScore, fpWordScore) = (0.0,0.0)  
          //RULE: word indicates LDA topic description
          if (docsAsHighValueTerms.lookup(id).contains(w))
            ldaWordScore = 10.0
          //RULE: word indicates FP description
          if(docsAsFPTerms.lookup(id).contains(w))
            fpWordScore = 10.0
          //VALUE for term in Range [0,400]
          (ldaWordScore + fpWordScore).toDouble
        }
       (id, severityScore + priorityScore + updatedNumericTokens.sum) 
      }
    
    // for each word in a case check - product, component, OS, version
    val products = e_product.distinct().map(_._2.what).zipWithIndex()
    val components = e_component.distinct().map(_._2.what).zipWithIndex()
    val os = e_op_sys.distinct().map(_._2.what).zipWithIndex()
    val versions = e_version.distinct().map(_._2.what).zipWithIndex()
    val givenValues = data.keys.map { key =>  
      var (productScore, componentScore, osScore, versionScore) = (0.0,0.0,0.0,0.0)  
      val details : List[String] = List(e_product.lookup(key).head.what, e_component.lookup(key).head.what, e_op_sys.lookup(key).head.what, e_version.lookup(key).head.what)
      productScore = products.lookup(details.apply(0)).head
      componentScore = components.lookup(details.apply(1)).head
      osScore = os.lookup(details.apply(2)).head
      versionScore = versions.lookup(details.apply(3)).head
      val value = (productScore + componentScore + osScore + versionScore).toDouble
      (key, value)
    }
    
    // Finally, each bug is converted into a (x,y) point on a 2D plane
    val finalValues = givenValues.join(computedValues)
    finalValues.cache()
    val points = finalValues.map(row => {
        val point = Vectors.dense(row._2._1, row._2._2) //(x,y)=(given, predicted)
        (row._1, point)
      }
    )
    points.cache()
    println("Complete: getting all dimension data")
    
    // Use k-means to find prior classes
    var numClusters = 20 //good value = number of distinct product series times avg number of problems per product
    var numIterations = 20  
    var priors: List[KMeansModel] = List()
    var errors: List[Double] = List()
    /* need testing: we don't know how many clusters beforehand 
     * so keep clustering until when error is below threshold
     * just add that check condition in while loop below 
     * example: while(errors.isEmpty && errors.last < 5) */
    while(errors.isEmpty){ 
      numClusters+=5
      priors = priors :+ KMeans.train(points.values, numClusters, numIterations)
      errors = errors :+ priors.last.computeCost(points.values)
    }
    val kmeansPredictions = points
      .map {point => (priors.last.predict(point._2) -> List(point._1)) }
      .reduceByKey(_++_)
    val x = points.map(f => (f._1 -> f._2.apply(0))).collect().toMap
    val y = points.map(f => (f._1 -> f._2.apply(1))).collect().toMap
    val outputClusters = kmeansPredictions
      .filter(cluster => cluster._2.size>1)//clusters that matter
      .map{ cluster => 
        val outputVal = cluster._2.map(id => Array(id, x.getOrElse(id, "x not found").toString(), y.getOrElse(id, "y not found").toString()))
        (cluster._1.toString(), outputVal)  
        } 
    val finalError = errors.last
    println(s"Complete: K means with an error of $finalError")
      
    
/*
 * 
 * 
 * 
 * 
 * 
 * 
 *     
    
    // step 2: supervised learning
    // get new bug history from wherever thru kafka to spark streams
    Properties props = new Properties();
    props.put("zk.connect", "127.0.0.1:2181");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    ProducerConfig config = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<String, String>(config);
    ProducerData<String, String> data = new ProducerData<String, String>("test-topic", "test-message");
    producer.send(data);  
    val topic = "test"
    val zk = "localhost:2181"
    val broker = "localhost:9092"
    val sparkConf1 = new SparkConf()
    val sc1 = new SparkContext("local[2]","Spark-Streams-from-Kafka",sparkConf1)
    val ssc = new StreamingContext(sc1, Seconds(10))
    val kafkaConf = Map(
        "metadata.broker.list" -> broker,
        "auto.offset.reset" -> "smallest",
        "zookeeper.connect" -> zk,
        "group.id" -> "spark-streaming-from-kafka",
        "zookeeper.connection.timeout.ms" -> "2500")

    /*Direct Stream from Kafka to Spark*/
    val dstreamFromKafka = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaConf, Set(topic)).map(_._2)
    val newBugDescs = dstreamFromKafka.map { newbug =>
        // do the same stuff we did above for cleaning
        (bugID, cleanWords)
        }
    
    /* Getting Kafka offsets from RDDs */
    dstreamFromKafka.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { partitionOfRecords =>
        partitionOfRecords.foreach(record => println(record)
        }
      }
    
    /* Create naive bayes for matching new bug with clusters from step 1 */
    val model = NaiveBayes.train(prior, lambda = 1.0, modelType = "multinomial")
    val predictionAndLabel = newStuff.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    val wordsFromKafka = dstreamFromKafka.flatMap(_.split(" "))
    val counts = wordsFromKafka.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(5), Seconds(10), 2)
    
    ssc.checkpoint("./checkpoints")  
    ssc.start()
    ssc.awaitTermination()

* 
* 
* 
* 
*  
*/

  }

}

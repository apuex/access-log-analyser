package com.github.apuex.accesslog.analyser

import java.io._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic._
import java.util.regex._

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import com.github.apuex.accesslog.ApacheHttpdParser._
import com.github.apuex.accesslog.Field._
import com.github.apuex.springbootsolution.runtime._

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration.FiniteDuration

object Main extends App {
  implicit val system = ActorSystem("system")
  implicit val materializer = ActorMaterializer()
  implicit val execuionContext = system.dispatcher


  val lineCount = new AtomicLong(0)
  val parsedCount = new AtomicLong(0)
  val bodyLengthTotal = new AtomicLong(0)

  if (args.length == 1) {
    val (s1, s2) = analyse()
    val futures = Seq(
      s1,
      s2
    )

    var requestMap = new mutable.TreeMap[String, Long]()
    var userAgentMap = new mutable.TreeMap[String, Long]()

    def updateRequestMap(x: (String, Long)) = synchronized {
      requestMap += (x._1 -> (requestMap.getOrElse(x._1, 0L) + x._2))
    }

    def updateUserAgentMap(x: (String, Long)) = synchronized {
      userAgentMap += (x._1 -> (userAgentMap.getOrElse(x._1, 0L) + x._2))
    }

    s1.map(s => s.foreach(updateRequestMap(_)))
        .map(_ => {
          val requestsWriter = new PrintWriter(new FileOutputStream("requests-on-uri.log"))
          requestMap.toArray
            .sortWith((x, y) => x._2 > y._2)
            .foreach(x => requestsWriter.println(s"${x._2}, ${x._1}"))

          requestsWriter.flush()
          requestsWriter.close()
        })
    s2.map(s => s.foreach(updateUserAgentMap(_)))
        .map(_ => {
          val userAgentWriter = new PrintWriter(new FileOutputStream("requests-by-user-agent.log"))
          userAgentMap.toArray
            .sortWith((x, y) => x._2 > y._2)
            .foreach(x => userAgentWriter.println(s"${x._2}, ${x._1}"))

          userAgentWriter.flush()
          userAgentWriter.close()
        })

    futures.foreach(f => Await.result(f, FiniteDuration(1000, TimeUnit.DAYS)))


    println(s"line count: ${lineCount}")
    println(s"parsed count: ${parsedCount}")
    println(s"body length total: ${bodyLengthTotal}")

    materializer.shutdown()
    system.terminate()

  } else {
    println("usage: <cmd> <log dir>")
    materializer.shutdown()
    system.terminate()
  }

  def analyse() = {
    val requestSink = Sink.seq[(String, Long)]
    val userAgentSink = Sink.seq[(String, Long)]
    RunnableGraph.fromGraph(GraphDSL.create(requestSink, userAgentSink)((_, _)) { implicit builder =>
      (theRequestSink, theUserAgentSink) =>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val logs = accessLog(new File(args(0)))

        val userAgentColIndex = combined.indexOf(UserAgent)
        val requestColIndex = combined.indexOf(Request)
        val bodyLengthIndex = combined.indexOf(BodyLength)

        val tokenizer = Flow[String]
          .map(x => {
            lineCount.incrementAndGet()
            x
          })
          .map(x => tokenLine(x, combinedPattern).filter(_ != null)).filter(!_.isEmpty)
          .map(x => {
            parsedCount.incrementAndGet()
            try {
              bodyLengthTotal.addAndGet(parseLong(x(bodyLengthIndex)))
            } catch {
              case x: Throwable => x.printStackTrace()
            }
            x
          })
        val broadcast = builder.add(Broadcast[Array[String]](2))
        val request = Flow[Array[String]]
          .map(x => (x(requestColIndex), parseLong(x(bodyLengthIndex))))
          .map(x => {
            val request = x._1.split("\\s+")
            if (request.length > 1) (request(1).split("[\\?]")(0), x._2)
            else {
              x
            }
          })
        val userAgent = Flow[Array[String]]
          .map(x => (x(userAgentColIndex), parseLong(x(bodyLengthIndex))))

        logs ~> tokenizer ~> broadcast ~> userAgent ~> theUserAgentSink.in

        broadcast ~> request ~> theRequestSink.in

        ClosedShape
    }).run()(materializer)
  }

  private def accessLog(archiveDir: File): Source[String, NotUsed] = {
    Source.fromIterator(() => archiveDir.list((_, name) => {
      val p: Pattern = Pattern.compile("(access)(.*)")
      val m: Matcher = p.matcher(name)
      m.matches()
    }).sorted(Ordering.String).iterator)
      .flatMapConcat(f => {
        val reader = new BufferedReader(new InputStreamReader(new FileInputStream("%s%s%s".format(archiveDir.getPath, File.separator, f))))
        Source.fromIterator[String](() => new ScalaLineIterator(reader))
      })
  }

  private def parseLong(text: String): Long = {
    try {
      text.toLong
    } catch {
      case _: Throwable => 0L
    }
  }
}

package com.github.apuex.accesslog.analyser

import java.io._
import java.util.regex._
import com.github.apuex.accesslog.ApacheHttpdParser._
import com.github.apuex.accesslog.Field._
import com.github.apuex.springbootsolution.runtime._
import akka._
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import akka.NotUsed
import akka.actor.ActorSystem
import akka.event._

import scala.concurrent._

object Main extends App {
  implicit val system = ActorSystem("system")
  implicit val materializer = ActorMaterializer()
  implicit val execuionContext = system.dispatcher


  if(args.length == 1) {
    analyse().map(s => {
      s.toSet[String].foreach(x => println(x))
      materializer.shutdown()
      system.terminate()
    })
  } else {
    println("usage: <cmd> <log dir>")
  }

  def analyse(): Future[Seq[String]] = {
    val userAgentSink = Sink.seq[String]
    RunnableGraph.fromGraph(GraphDSL.create(userAgentSink) { implicit builder =>
      (theUserAgentSink) =>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val logs = accessLog(new File(args(0)))
      val tokenizer = Flow[String].map(x => tokenLine(x, combinedPattern).filter(_ != null)).filter(!_.isEmpty)
      //val broadcast = builder.add(Broadcast[Array[String]](1))
      val userAgentColIndex = combined.indexOf(UserAgent)
      val requestColIndex = combined.indexOf(Request)
      //val request = Flow[Array[String]].map(x => x(requestColIndex))
      val userAgent = Flow[Array[String]].map(x => x(userAgentColIndex))
          .log("error")

      logs ~> tokenizer ~> userAgent ~> theUserAgentSink.in
       // Sink.foreach[String](x => println(x))

      //broadcast ~> request ~> Sink.foreach[String](x => println(x))

      ClosedShape
    }).run()(materializer)
  }

  def accessLog(archiveDir: File): Source[String, NotUsed] = {
    Source.fromIterator(() => archiveDir.list((_, name) => {
      val p: Pattern = Pattern.compile("(access)(.*)")
      val m: Matcher = p.matcher(name)
      m.matches()
    }).sorted(Ordering.String).iterator)
      .flatMapConcat(f => {
        //println(f)
        val reader = new BufferedReader(new InputStreamReader(new FileInputStream("%s%s%s".format(archiveDir.getPath, File.separator, f))))
        Source.fromIterator[String](() => new ScalaLineIterator(reader))
      })
      .recover({
        case x => x.printStackTrace()
          "stream truncated"
      })
  }
}

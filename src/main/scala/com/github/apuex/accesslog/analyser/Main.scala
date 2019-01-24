package com.github.apuex.accesslog.analyser

import java.io._
import java.util.concurrent.TimeUnit
import java.util.regex._

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import com.github.apuex.accesslog.ApacheHttpdParser._
import com.github.apuex.accesslog.Field._
import com.github.apuex.springbootsolution.runtime._

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration

object Main extends App {
  implicit val system = ActorSystem("system")
  implicit val materializer = ActorMaterializer()
  implicit val execuionContext = system.dispatcher


  if (args.length == 1) {
    val (s1, s2) = analyse()
    val futures = Seq(s1, s2)
    futures.foreach(s => s.map(s => s.toSet[String].foreach(x => println(x))))
    futures.foreach(f => Await.ready(f, FiniteDuration(1000, TimeUnit.DAYS)))
    materializer.shutdown()
    system.terminate()
  } else {
    println("usage: <cmd> <log dir>")
    materializer.shutdown()
    system.terminate()
  }

  def analyse(): (Future[Seq[String]], Future[Seq[String]]) = {
    val requestSink = Sink.seq[String]
    val userAgentSink = Sink.seq[String]
    RunnableGraph.fromGraph(GraphDSL.create(requestSink, userAgentSink)((_, _)) { implicit builder =>
      (theRequestSink, theUserAgentSink) =>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val logs = accessLog(new File(args(0)))
        val tokenizer = Flow[String].map(x => tokenLine(x, combinedPattern).filter(_ != null)).filter(!_.isEmpty)
        val broadcast = builder.add(Broadcast[Array[String]](2))
        val userAgentColIndex = combined.indexOf(UserAgent)
        val requestColIndex = combined.indexOf(Request)
        val request = Flow[Array[String]].map(x => x(requestColIndex))
        val userAgent = Flow[Array[String]].map(x => x(userAgentColIndex))
          .log("error")

        logs ~> tokenizer ~> broadcast ~> userAgent ~> theUserAgentSink.in

        broadcast ~> request ~> theRequestSink.in

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

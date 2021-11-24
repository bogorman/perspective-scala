package com.bogorman.perspective_scala

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.WebSocketRequest
import akka.http.scaladsl.model.ws.Message
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse

import scala.io.StdIn

import concurrent.Future
import scala.util.Success
import scala.util.Failure

object Main extends App {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  // ORDERS_UPDATE_QUEUE = Queue()

  try {
    // should throw exception if something is not setup correctly
    Perspective.verify()

    //load from config
    val perspectiveWSPort = 8081
    val externalWSPort = 8080

    val ordersTableSchema = Map(
      "id" -> "int",
      "symbol" -> "str",
      "type" -> "str",
      "reason" -> "str",
      "side" -> "str",
      "price" -> "float",
      "delta" -> "float",
      "remaining" -> "float" //,
    )

    val ordersTableDefn =
      Perspective.TableDefinition(
        "orders_table",
        ordersTableSchema,
        index = Some("id")
      )

    val tradesTableSchema = Map(
      "id" -> "int",
      "symbol" -> "str",
      "type" -> "str",
      "reason" -> "str",
      "side" -> "str",
      "price" -> "float",
      "delta" -> "float",
      "remaining" -> "float",
      "valid" -> "boolean",
      "trade_date" -> "date",
      "timestamp" -> "datetime"
    )

    val tradesTableDefn =
      Perspective.TableDefinition(
        "trades_table",
        tradesTableSchema,
        index = Some("id")
      )

    Perspective.hostTables(List(ordersTableDefn, tradesTableDefn))

    implicit val ec: scala.concurrent.ExecutionContext =
      scala.concurrent.ExecutionContext.global

    println("Starting perspective server")
    Perspective.start(perspectiveWSPort)

    //generate fake data

    val f = Future {
      val r = scala.util.Random

      while (true) {

        val orderData = Map(
          "symbol" -> "ETH",
          "type" -> "CRYPTO",
          "reason" -> "XXX",
          "side" -> "BUY",
          "price" -> r.nextFloat,
          "delta" -> r.nextFloat,
          "remaining" -> r.nextFloat
        )

        Perspective.updateTable("orders_table", orderData)

        Perspective.persistTable(
          "orders_table"
        )

        // Perspective.restoreTable(
        //   "orders_table"
        // )

        val tradeData = Map(
          "symbol" -> "ETH-TRADE",
          "type" -> "CRYPTO",
          "reason" -> "XXX",
          "side" -> "BUY",
          "price" -> r.nextFloat,
          "delta" -> r.nextFloat,
          "remaining" -> r.nextFloat,
          "valid" -> r.nextBoolean,
          "trade_date" -> new java.util.Date(),
          "timestamp" -> new java.util.Date()
        )
        Perspective.updateTable(
          "trades_table",
          tradeData
        )

        Perspective.persistTable(
          "trades_table"
        )

        // Perspective.restoreTable(
        //   "trades_table"
        // )

        Thread.sleep(10)
      }
    }
    f.onComplete {
      case Success(x) ⇒ {
        println("Future ended success")
        println(x)
      }
      case Failure(e) ⇒ {
        println("Future failed")
        println(e)
      }
    }

    println("Press RETURN to stop...")
    StdIn.readLine()

  } catch {
    case e: Exception ⇒ {
      println("APP EXCEPTION" + e.getMessage)
      throw e
    }
  }
}

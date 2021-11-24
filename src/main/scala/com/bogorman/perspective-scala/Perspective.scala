package com.bogorman.perspective_scala

import me.shadaj.scalapy.py
import me.shadaj.scalapy.interpreter.PyValue
import me.shadaj.scalapy.interpreter.CPythonInterpreter
import me.shadaj.scalapy.py.SeqConverters

import java.io.File
import java.io.FileWriter

import concurrent.Future
import scala.util.Success
import scala.util.Failure

object Perspective {
  lazy val debugFile = File.createTempFile("debug.py", null)
  var PERSIST_FOLDER = "" //needs to be set before hostTables is called.

  case class TableDefinition(
      name: String,
      schema: Map[String, String],
      index: Option[String] = None,
      limit: Option[Int] = None
  )

  def verify() = {
    val listLengthPython =
      py.Dynamic.global.len(List(1, 2, 3, 4, 5).toPythonProxy)
    val listLength = listLengthPython.as[Int]

    if (listLength != 5) {
      throw new Exception("ScalaPy not setup correctly.")
    } else {
      println("VERIFIED")
    }

    //do the imports
    doImports()
    setupLogging()
    setupPersistance()
    setupManager()

    println("Creating debug file at:" + debugFile.getAbsolutePath())
  }

  def hostTables(tableDefinitions: List[TableDefinition]) = {
    tableDefinitions.foreach { td =>
      setupTable(td)
      setupOrderTableUpdateQueue(td.name)
      setupTableUpdateThreads(td.name)
      setupTablePersistance(td.name)
      setupTableRestore(td.name)
      doTableRestore(td.name)
    }

    setupPerspectiveThreadForTables(tableDefinitions)
  }

  def start(
      websocketPort: Int
  )(implicit ec: scala.concurrent.ExecutionContext) = {
    setupTornadoApp()

    val f = Future {
      //once this starts it should not end
      println("STARTING PERSPECTIVE SERVER")
      setupApp(websocketPort)
      println("ENDED PERSPECTIVE SERVER")
    }
    f.onComplete {
      case Success(x) ⇒ {
        println("PERSPECTIVE Future ended success")
        println(x)
      }
      case Failure(e) ⇒ {
        println("PERSPECTIVE Future failed")
        println(e)
      }
    }
  }

  def decodeValueToPy(value: Any): py.Any = {
    value match {
      case x: Double         => py.Any.from[Double](x)
      case x: Float          => py.Any.from[Float](x)
      case x: Long           => py.Any.from[Long](x)
      case x: Int            => py.Any.from[Int](x)
      case x: String         => py.Any.from[String](x)
      case x: java.util.Date => py.Any.from[Long](x.getTime)
      case x: Boolean        => py.Any.from[Boolean](x)

      case x => {
        throw new Exception("Unhandled type:" + x)
      }
    }
  }

  def updateTable(tableName: String, row: Map[String, Any]) = {
    val qName = s"${tableName}_update_queue"

    // py.local {
    val q = py.Dynamic.global.selectDynamic(qName)
    val rowDict = py.Dynamic.global.dict()
    row.foreach { case (key, value) =>
      {
        rowDict.bracketUpdate(key, decodeValueToPy(value))
      }
      // }
      //   q.put_nowait(rowDict)
      q.put(rowDict)
    }
  }

  def persistTable(tableName: String) = {
    val qName = s"${tableName}_update_queue"

    // py.local {
    val q = py.Dynamic.global.selectDynamic(qName)
    //   q.put_nowait(s"persist_${tableName}")
    q.put(s"persist_${tableName}")
    // }
  }

  def restoreTable(tableName: String) = {
    val qName = s"${tableName}_update_queue"

    // py.local {
    val q = py.Dynamic.global.selectDynamic(qName)
    //   q.put_nowait(s"restore_${tableName}")
    q.put(s"restore_${tableName}")
    // }
  }

  private def doImports() = {
    execManyLines(
      """import perspective
	  |#from queue import Queue
	  |import multiprocessing as mp
	  |from threading import Thread 
	  |from datetime import datetime
   	  |import tornado
      |import asyncio
   	  |import pickle
	  """.stripMargin
    )
  }

  private def setupLogging() = {
    execManyLines(
      """import logging
		|logging.basicConfig(
		|     level=logging.DEBUG,
		|     format="%(asctime)s - [%(threadName)-12.12s] %(process)d %(levelname)s: %(message)s",
		|     handlers=[
		|         logging.FileHandler("perspective_embed.log")
		|     ]
  		|)
	  """.stripMargin
    )
  }

  private def setupPersistance() = {
    // execManyLines(
    //   s"""
    //   |
    //   """.stripMargin
    // )
  }

  private def setupManager() = {
    execManyLines(
      """manager = perspective.PerspectiveManager()
		|logging.info("Creating psp_loop")
	    |psp_loop = tornado.ioloop.IOLoop()	  
	  """.stripMargin
    )
    py.Dynamic.global.manager
  }

  //https://stackoverflow.com/questions/27948128/how-to-convert-scala-map-into-json-string
  //should use some proper json library
  def toJson(query: Any): String = query match {
    case m: Map[String, Any] => s"{${m.map(toJson(_)).mkString(",")}}"
    case t: (String, Any)    => s""""${t._1}":${toJson(t._2)}"""
    case ss: Seq[Any]        => s"""[${ss.map(toJson(_)).mkString(",")}]"""
    case s: String           => s""""$s""""
    case null                => "null"
    case _                   => query.toString
  }

  private def toSchemaJson(schema: Map[String, String]): String = {
    toJson(schema)
  }

  private def quote(q: String) = {
    s"'${q}'"
  }

//   lazy val perspective = py.module("perspective")
  private def setupTable(
      tableDefn: TableDefinition
  ) = {
    val schema = tableDefn.schema
    val index = tableDefn.index.map(quote(_)).getOrElse("None")
    val limit = tableDefn.limit.map(_.toString).getOrElse("None")

    println(toSchemaJson(schema))
    execManyLines(s"""
     |${tableDefn.name} = perspective.Table(${toSchemaJson(schema)}, index=${index}, limit=${limit})
	""".stripMargin)
  }

  private def setupOrderTableUpdateQueue(tableName: String) = {
    execManyLines(
      s"""${tableName}_update_queue = mp.Queue()
	  """
    )

    val qName = s"${tableName}_update_queue"
    val q = py.Dynamic.global.selectDynamic(qName)
    q
  }

  private def setupPerspectiveThreadForTables(
      tableDefinitions: List[TableDefinition]
  ) = {
    // val lines = MList.empty[String]
    val lines = new scala.collection.mutable.ArrayDeque[String]
    lines += "def perspective_thread(manager):"
    lines += "    manager.set_loop_callback(psp_loop.add_callback)"
    tableDefinitions.foreach { td =>
      lines += s"    manager.host_table(\"${td.name}\", ${td.name})"
    }
    lines += "    psp_loop.start()"

    execManyLines(lines.mkString("\n"))
  }

  private def setupTornadoApp() = {
    execManyLines(
      """def make_app():
	    |    thread = Thread(target=perspective_thread, args=(manager,))
	    |    thread.daemon = True
	    |    thread.start()
	    |
	    |    return tornado.web.Application(
	    |        [
	    |            (
	    |                r"/websocket",
	    |                perspective.PerspectiveTornadoHandler,
	    |                {"manager": manager, "check_origin": True},
	    |            )
	    |        ]
	    |    )    
	  """.stripMargin
    )
  }

  private def setupApp(port: Int) = {
    execManyLines(
      s"""
		|def make_app():
		|    thread = Thread(target=perspective_thread, args=(manager,))
		|    thread.daemon = True
		|    thread.start()
		|
		|    return tornado.web.Application(
		|        [
		|            (
		|                r"/websocket",
		|                perspective.PerspectiveTornadoHandler,
		|                {"manager": manager, "check_origin": True},
		|            )
		|        ]
		|    )    
		|      
		|def run_app():
		|    asyncio.set_event_loop(asyncio.new_event_loop())
		|    # ws.run()
		|    app = make_app()
  		|    logging.critical("Perspective running on http://localhost:${port}")
		|    app.listen(${port})
		|    tornado.ioloop.IOLoop.instance().start()
		|
		|
		|t = Thread(target=run_app, args=())
		|t.daemon = True
		|logging.info("starting thread..")
		|t.start()
		|
		|logging.info("joining...")
		|t.join()
	  """.stripMargin
    )
  }

  private def setupTablePersistance(tableName: String) = {
    execManyLines(
      s"""
		|def do_persist_${tableName}():
    |	logging.info("do_persist_${tableName} - START")
		|	t = ${tableName}.view().to_arrow()
    |	logging.info(f"DUMPING ${tableName} row count: {${tableName}.size()}")
    |	pickle.dump(t, open("${PERSIST_FOLDER}${tableName}.bin", "wb"))
    |	logging.info("do_persist_${tableName} - END")
		""".stripMargin
    )
  }

  private def setupTableRestore(tableName: String) = {
    execManyLines(
      s"""
		|def do_restore_${tableName}():
		|	try:
    |		logging.info("do_restore_${tableName} - START")
    |		t = pickle.load(open("${PERSIST_FOLDER}${tableName}.bin", "rb"))
		|		${tableName}.replace(t)
    |		logging.info(f"RESTORED ${tableName} row count: {${tableName}.size()}")
    |		logging.info("do_restore_${tableName} - END")
		|	except Exception as e:
		|		logging.error(e)
		""".stripMargin
    )
  }

  private def doTableRestore(tableName: String) = {
    execManyLines(
      s"""
      |logging.info("AUTO RESTORING TABLE START")
      |do_restore_${tableName}()
      |logging.info("AUTO RESTORING TABLE DONE")
      """.stripMargin
    )
  }

  private def setupTableUpdateThreads(tableName: String) = {
    val qName = s"${tableName}_update_queue"

    execManyLines(
      s"""
	  |def ${tableName}_table_updater(event_queue):
	  |    while True:
	  |        try:
	  |            data = event_queue.get()
    |            if (isinstance(data, str) and data == "persist_${tableName}"):
    |                psp_loop.add_callback(do_persist_${tableName})
    |            elif (isinstance(data, str) and data == "restore_${tableName}"):
    |                psp_loop.add_callback(do_restore_${tableName})
    |            else: 	
    |                psp_loop.add_callback(${tableName}.update, [data])
	  |
	  |        except Exception as e:
	  |            logging.error(e)
	  |
	  |table_update_thread = Thread(target=${tableName}_table_updater, args=(${qName},))
	  |table_update_thread.daemon = True
	  |table_update_thread.start()
	  """.stripMargin
    )
  }

  private def execManyLines(lines: String) = {
    debug(lines)

    CPythonInterpreter.execManyLines(lines)
  }

  def debug(lines: String) = {
    val debug = true
    if (debug) {
      val fw = new FileWriter(debugFile.getAbsolutePath(), true)
      fw.write(lines + "\n")
      fw.close()
    }
  }

  def cleanup() = {}

}

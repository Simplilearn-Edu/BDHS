package com.simplilearn.bigdata.casestudy_7

import scala.collection.mutable.ListBuffer
import scala.io.Source

object Solution_1 {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      System.out.println("Please provide <input_path>")
      System.exit(0)
    }

    val filename = args(0)

    var compaines = new ListBuffer[String]()
    for (line <- Source.fromFile(filename).getLines) {
      val arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
      if(!arr(0).equalsIgnoreCase("name")) {
        if(arr(2).trim.length > 0 && arr(2).toInt < 1980) {
          compaines += arr(0)
        }
      }
    }

    System.out.println("Total Companies Count = "+compaines.size)
    System.out.println("Companies = "+compaines)
  }

}

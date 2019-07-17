package com.simplilearn.bigdata.casestudy_7

import scala.collection.mutable
import scala.io.Source

object Solution_3 {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      System.out.println("Please provide <input_path>")
      System.exit(0)
    }

    val filename = args(0)

    val mapMut = scala.collection.mutable.Map[String, Int]()

    for (line <- Source.fromFile(filename).getLines) {
      val arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
      if(!arr(0).equalsIgnoreCase("name")) {
        if(mapMut.contains(arr(3))) {
          mapMut += (arr(3) -> ("1".toInt + mapMut.get(arr(3)).get))
        }else {
          mapMut += (arr(3) -> "1".toInt)
        }
      }
    }
    System.out.println("Top 5 industry = "+mapMut.toSeq.sortBy(-_._2).toList.take(5))
  }
}

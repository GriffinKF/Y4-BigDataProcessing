import scala.io.Source
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable.Map;
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.language.postfixOps

object Mapper {
  // Counts the number of each character in a string using a loop
  def CountChars(lines: Iterator[String]):Map[Char,Int] = {
    val map = Map[Char,Int]()
    lines.foreach(line =>
      line.toLowerCase.foreach(
        letter =>
          if(letter.isLetter) {
            val value = map.getOrElse(letter, 0) + 1
            map(letter) = value
          }
      )
    )
    map
  }

  // Combines maps of chars with values for that map
  def CombineMap(m1:Map[Char, Int], m2:Map[Char, Int]):Map[Char, Int] = {
    m2.foreach(letter =>
      if(letter == letter) {
        val value = m1.getOrElse(letter._1, 0) + letter._2
        m1(letter._1) = value
      }
    )
    m1
  }

  // Pretty obvious
  def PrintMap(map: Map[Char,Int])  {
    map.foreach(i => {
      println(i._1 + ":" + i._2)
    })
  }
}

case class InputList(l : List[String])

//This is the start of the actors
class MainActor extends Actor {
  var sendResultTo : ActorRef = context.self

  def receive : Receive = {
    case  k : List[String] =>
      sendResultTo = sender()
      val firstSortingActor = context.actorOf (Props(new SortingActor(k)))
    case l : Map[Char, Int] =>
      sendResultTo ! l
  }
}

// This is the class for sorting actors, who do the actual work
class SortingActor(inputList : List[String]) extends Actor {

  var firstMap : Map[Char, Int] = Map()
  var mapsReceived : Int = 0
  //This if is entered at the bottom of the "tree" like structure that is created. At this point the actor only has one line, so it does not split into more more actors as there are no more lines to hand off
  if (inputList.length <= 1) {
    //Since it only has one line, it counts the character occurences in that line and saves it as a map
    val countedChars = Mapper.CountChars(inputList.toIterator)
    //It then sends the map to it's parent
    context.parent ! countedChars
  }
    //This else is entered when there are still more than one line in the list, at this point it's still possible to create more child actors and send them the list
  else {
    //This finds the "index" of the middle of the list
    var middle = inputList.length/2;
    //This uses splitAt and the previously calculated middle value to split the list in half, and save those split halves into pivot
    var pivot = inputList.splitAt(middle)
    //Create a new child actor, send them the left side of the pivot
    val actorLeft = context.actorOf (Props(new SortingActor(pivot._1)))
    //Create a new child actor, send them the right side of the pivot
    val actorRight = context.actorOf (Props(new SortingActor(pivot._2)))
  }

  //This is the parent Actors recieving a message back from the child actors after the child actors have counted the characters on a single line and put them into a map
  def receive: Receive = {

        //Case L is where it is the first child to send a message back, at this point it doesn't do anything as it still needs to wait for the second child to send the message back
    case l : Map[Char, Int] =>
      if (mapsReceived == 0) {
        firstMap = l
        mapsReceived += 1
      }
        //This is when the second child sends the message back, at this point it combines the results that each of the child have calculated and sends it up to it's parent
      else {
        val finalMap = Mapper.CombineMap(firstMap, l)
        context.parent ! finalMap
      }
  }
}

object Main extends App {
  //This line reads from the txt file and places the data in the immutable variable dataSource
  val dataSource = Source.fromFile("C:/Users/griff/Documents/School Work/Uni Work/Fourth Year/AC41011 - Big Data Analysis/Assignment 2 - Big Data Processing/Assignment/Part 1/text.txt");
  //Places the data from dataSource into another variable with lines as in the txt file.
  var lineList = dataSource.getLines().toList
  //Creates the actors system
  val s = ActorSystem("actor-system")
  val mainActor = s.actorOf(Props[MainActor])
  val timeout = Timeout (FiniteDuration(Duration("15 seconds").toSeconds, SECONDS))
  val future = ask (mainActor,lineList) (timeout)
  val result = Await.result (future, timeout.duration)
  //Simple Print for the result
  println ("The Final Result Is: " + result)
  s.terminate()
}




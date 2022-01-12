import org.apache.spark.SparkContext._
import sqlContext.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

// This line sets up the val input to use CSV Headers as its format, and reads in from the HDFS (This was done using WSL)
val input = spark.read.option("header",true).csv("hdfs://localhost:9000/user/root/*")

// Picking and saving the relevant data from the file saved to input for both the Home and Away occurences of each team. Also renames the fields for ease of use.
val homeGoals = input.select("HomeTeam","FTHG").toDF("Teams", "Goals")
val awayGoals = input.select("AwayTeam","FTAG").toDF("Teams", "Goals")

// Uses a "Union" to combine all the goals scored both home and away for each team. Also groups by the team name (since there would be about 10 occurences of each team, home and away for 5 seasons). Also sets the goals to an Int using a cast
val finalGoals = homeGoals.union(awayGoals).toDF("Team", "Goals").withColumn("Goals",col("Goals").cast(IntegerType)).groupBy("Team").agg(sum("Goals"))

// Saves the answer/analysis/solution back to the HDFS as a CSV File.
finalGoal.write.format("csv").save("Results")
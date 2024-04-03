intiate the spark session by using spark builder command, it is a master session

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()
![image](https://github.com/srimanth496/End-to-End-spark/assets/84217751/2fbc3f63-d782-42b4-b313-4c91c8d03a01)

Read csv file  from the system by using spark commands
![image](https://github.com/srimanth496/End-to-End-spark/assets/84217751/a7292ea8-a80a-4abb-98d1-54fa25560647)

After reading file from the system , dataframe is created and perform transformations and actions by using spark.
take function we can display rows in the dataframe
flightData2015.take(3)
![image](https://github.com/srimanth496/End-to-End-spark/assets/84217751/14f20717-f767-40f1-99c4-d0c230899c91)

sort function is for create new dataframe and sort based on the count
flightData2015.sort("count").explain()
![image](https://github.com/srimanth496/End-to-End-spark/assets/84217751/779ef275-0204-4a00-913b-adef688d3fb1)

. sorted the data based on the count by asscending order
![image](https://github.com/srimanth496/End-to-End-spark/assets/84217751/fd0c170f-c043-4777-9391-2cf7c02ad134)

display all the from the file in spark by using show command
flightData2015.sort("count").show()
![image](https://github.com/srimanth496/End-to-End-spark/assets/84217751/aaa882f9-d615-497c-86ac-5483d20aee05)

in spark convert dataframe to table formate and perform sql commands on the table .
flightData2015.createOrReplaceTempView("flight_data_2015")

for display max count in the table using 
spark.sql("SELECT max(count) from flight_data_2015").take(1)
![image](https://github.com/srimanth496/End-to-End-spark/assets/84217751/13891eba-0ac6-441c-a1b2-07e700d291da)









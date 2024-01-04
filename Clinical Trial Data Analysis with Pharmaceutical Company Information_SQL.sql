-- Databricks notebook source
-- MAGIC %python
-- MAGIC clinicaltrial_2021 = spark.read.option("header", True).option("escape",'\"').option("sep", "|").csv("/FileStore/tables/clinicaltrial_2021.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2021.createOrReplaceTempView ("clinicaltrial_2021")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharma = spark.read.option("header", True).option("escape",'\"').csv("/FileStore/tables/pharma.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharma.createOrReplaceTempView ("pharma")

-- COMMAND ----------

SELECT * From clinicaltrial_2021

-- COMMAND ----------

SELECT * From pharma LIMIT 5

-- COMMAND ----------

--  Answer to Question 1)  The number of studies in the dataset. You must ensure that you explicitly check distinct studies.

WITH
AA AS (SELECT Type, count(*) as frequency
FROM clinicaltrial_2021
WHERE Type != ''
GROUP BY Type)

SELECT SUM(frequency) as Total_Studies
FROM AA;

-- COMMAND ----------

-- Answer to Question 2
--list all the types (as contained in the Type column) of studies in the 
--dataset along with the frequencies of each type. These should be ordered from 
--most frequent to least frequent.


SELECT Type, count(*) as frequency
FROM clinicaltrial_2021
WHERE Type != ''
GROUP BY Type
ORDER BY frequency DESC


-- COMMAND ----------

--  Answer to Question (3)  The top 5 conditions (from Conditions) with their frequencies.

SELECT TRIM(subquery.Conditions) AS Conditions, COUNT(*) AS frequency
FROM  (SELECT EXPLODE(SPLIT(Conditions, ",")) AS Conditions FROM clinicaltrial_2021) subquery
GROUP BY Conditions
ORDER BY frequency DESC
LIMIT 5;

-- COMMAND ----------

--Answer to Question 4. Find the 10 most common sponsors that are not pharmaceutical companies, along 
--with the number of clinical trials they have sponsored. 

WITH Non_Pharmaceutical_Sponsors AS (
    SELECT c.sponsor AS Sponsor, COUNT(*) AS Num_Clinical_Trials
    FROM Clinicaltrial_2021 c
    LEFT JOIN Pharma p ON c.sponsor = p.company
    WHERE p.Parent_Company IS NULL
    GROUP BY c.sponsor
)
SELECT Sponsor, Num_Clinical_Trials
FROM Non_Pharmaceutical_Sponsors
ORDER BY Num_Clinical_Trials DESC
LIMIT 10;


-- COMMAND ----------


--- Question 5  
--Plot number of completed studies each month in a given year – for the submission 
--dataset, the year is 2021.
SELECT DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(LEFT(Completion, 3)),'MMM'), 'yyyy-MM-dd'), 'MM') as Months,
       Completion,
       COUNT(*) as Total_Studies
FROM clinicaltrial_2021
WHERE Completion LIKE '%2021%'
GROUP BY Completion
ORDER BY Months;


-- COMMAND ----------

--Extral feaututres.

---- (1)Question--What are the parent companies in the Pharma table that have conducted the most clinical trials according to the Clinicaltrial_2021 table, and what is the total number of trials for each of these companies?"    

--The statement in SQL is used to obtain information on how a query 
--is executed by the database engine. It shows the execution plan for the query, including the order of operations, the tables involved, and any --indexes or filters that are used.

-- COMMAND ----------

WITH cte AS (
    SELECT p.Parent_Company, COUNT(*) AS Total_Trials,
           RANK() OVER (PARTITION BY p.Parent_Company ORDER BY COUNT(*) DESC) AS Rank
    FROM Pharma p
    JOIN Clinicaltrial_2021 c ON p.Company = c.Sponsor
    GROUP BY p.Parent_Company
)
SELECT Parent_Company, Total_Trials
FROM cte
WHERE Rank = 1
ORDER BY Total_Trials DESC;


-- COMMAND ----------

--- (2)
----This SQL statement retrieves the total number of clinical trials for each unique sponsor from the Clinicaltrial_2021 table, and orders the result by the total number of trials in descending order

SELECT Sponsor, COUNT(*) AS TotalTrials
FROM Clinicaltrial_2021
GROUP BY Sponsor
ORDER BY TotalTrials DESC;

-- COMMAND ----------

----(3)
-- SQL query to retrieve parent company and total number of clinical trials for each company

SELECT p.Parent_Company, COUNT(*) AS TotalTrials
FROM Pharma p
JOIN Clinicaltrial_2021 c ON p.Company = c.Sponsor
GROUP BY p.Parent_Company
ORDER BY TotalTrials DESC;

-- COMMAND ----------



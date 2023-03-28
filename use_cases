----Create the OrderDetails Table

df = spark.read.format('com.crealytics.spark.excel') \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('dataAddress', "'Order Details'!") \
    .load('/FileStore/tables/northdb_csv.xlsx')

----Find The Sales Using The DataFrame---------

from pyspark.sql.functions import col,sum
df = df.withColumn("Sales", col("Quantity") * col("UnitPrice") * (1 - col("Discount")))

----write into the table-----------------------

df.write.format('csv').saveAsTable('OrderDetails')
total_sales = df.agg(sum("Sales")).collect()
display(total_sales)


------Create the Order table------------------

df = spark.read.format('com.crealytics.spark.excel') \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('dataAddress', "'Orders'!") \
    .load('/FileStore/tables/northdb_csv.xlsx')
    
df.write.format("delta").saveAsTable("Orders")

--------create the Employees table-----------

df = spark.read.format('com.crealytics.spark.excel') \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('dataAddress', "'Employees'!") \
    .load('/FileStore/tables/northdb_csv.xlsx')
df.write.format("delta").saveAsTable("Employees")

--------------Create the Shippers Table-------

df = spark.read.format('com.crealytics.spark.excel') \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('dataAddress', "'Shippers'!") \
    .load('/FileStore/tables/northdb_csv.xlsx')
df.write.format("delta").saveAsTable("Shippers")

-------------Create the Customers Table -------

df = spark.read.format('com.crealytics.spark.excel') \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('dataAddress', "'Customers'!") \
    .load('/FileStore/tables/northdb_csv.xlsx')
df.write.format("delta").saveAsTable("Customers")

----------Create the Customers Table----------

df = spark.read.format('com.crealytics.spark.excel') \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('dataAddress', "'Products'!") \
    .load('/FileStore/tables/northdb_csv.xlsx')
df.write.format("delta").saveAsTable("Products")

-----------Create the Suppliers Tables--------

df = spark.read.format('com.crealytics.spark.excel') \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('dataAddress', "'Suppliers'!") \
    .load('/FileStore/tables/northdb_csv.xlsx')
df.write.format("delta").saveAsTable("Suppliers")

--------Create the Categories table---------------

df = spark.read.format('com.crealytics.spark.excel') \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('dataAddress', "'Categories'!") \
    .load('/FileStore/tables/northdb_csv.xlsx')
df.write.format("delta").saveAsTable("Categories")


##########   Top 10 products in each Category  ############


%sql
SELECT c.CategoryID,c.CategoryName, p.ProductName, p.UnitPrice
FROM Categories c
INNER JOIN (
    SELECT CategoryID, ProductName, UnitPrice,
           ROW_NUMBER() OVER (PARTITION BY CategoryID ORDER BY UnitPrice DESC) AS rank
    FROM Products
) p ON c.CategoryID = p.CategoryID AND p.rank <= 10
ORDER BY c.CategoryName, p.UnitPrice DESC;

#######----Top 10 Customers By sales In Each Country------#######

%sql 
drop table if exists salesCustomer;
create table salesCustomer as select c.Country, o.CustomerID, SUM(od.UnitPrice * od.Quantity) AS TotalSales FROM Orders o JOIN OrderDetails od ON o.OrderID = od.OrderID JOIN Customers c ON o.CustomerID = c.CustomerID GROUP BY c.Country,o.CustomerID


#########-----Top 10 products in each Category------####

%sql
SELECT c.CategoryID,c.CategoryName, p.ProductName, p.UnitPrice
FROM Categories c
INNER JOIN (
    SELECT CategoryID, ProductName, UnitPrice,
           ROW_NUMBER() OVER (PARTITION BY CategoryID ORDER BY UnitPrice DESC) AS rank
    FROM Products
) p ON c.CategoryID = p.CategoryID AND p.rank <= 10
ORDER BY c.CategoryName, p.UnitPrice DESC;

-->
drop table if exists salesCustCountry;
create table salesCustcountry AS (SELECT Country, CustomerID, TotalSales, RANK() OVER (PARTITION BY Country ORDER BY TotalSales DESC) AS Rank
FROM salesCustomer);

-->
SELECT Country, CustomerID, TotalSales FROM salesCustCountry WHERE Rank <= 10 ORDER BY Country, TotalSales DESC;

##########-----Country and Year wise sales of Sub-Total-----######

%sql
SELECT 
  coalesce(Country, 'All') AS Country, 
  year(cast(OrderDate as date)) AS Year, 
  sum((Quantity * UnitPrice)) AS SubTotal 
FROM 
  (
    SELECT 
      c.Country, 
      o.OrderDate, 
      od.Quantity, 
      od.UnitPrice 
    FROM 
      Customers c 
      INNER JOIN Orders o ON c.CustomerID = o.CustomerID 
      INNER JOIN OrderDetails od ON o.OrderID = od.OrderID
  ) AS Sales 
GROUP BY 
  Country, 
  Year 
ORDER BY 
  Country ASC NULLS LAST, 
  Year ASC NULLS LAST;
  
 
 #######----------Common customers In Last Two Years -------#######
 
 %sql
create table maxyear as select * from Orders where year(OrderDate)=(select year(max(orderdate)) from Orders);

CREATE TABLE max1 AS
SELECT *
FROM Orders
WHERE YEAR(OrderDate) = (SELECT YEAR(MAX(OrderDate)) FROM Orders) - 1;
  
select distinct(m.customerID) from maxyear m join max1 m2 on m.customerID=m2.customerID
  

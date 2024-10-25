-- 2.1 Create Database
CREATE DATABASE BlueSkyOnlineDWH;
GO

--Use Database
USE BlueSkyOnlineDWH;
Go

----------------------------------------------------------------------------------------

--2.2 Create dimension tables
CREATE TABLE DimDates (
    FullDate DATE PRIMARY KEY NOT NULL,
    MonthofYear INT NULL,
    CalendarQuarter INT NULL,
    CalendarYear INT NULL
);

CREATE TABLE DimCustomer (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    CustomerCode VARCHAR(20) NOT NULL,
    FirstName VARCHAR(50) NOT NULL,
    LastName VARCHAR(50) NOT NULL,
    BirthDate DATE NOT NULL,
    MaritalStatus VARCHAR(10) NOT NULL,
    Gender CHAR(1) NOT NULL,
    PostCode VARCHAR(10) NULL,
    City VARCHAR(50) NULL,
    Income INT NULL
);

CREATE TABLE DimSellingChannel (
    SellingChannelKey INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    SellingChannelName VARCHAR(50) NOT NULL,
    SellingChannelCode VARCHAR(10) NULL,
    CommissionRate VARCHAR(20) NOT NULL
);

CREATE TABLE DimPaymentType (
    PaymentTypeKey INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    PaymentTypeName VARCHAR(50) NOT NULL,
    PaymentTypeID INT NOT NULL
);

-------------------------------------------------------------------------------------------------------------
-- 2.3 and 2.4 Create Fact Tables with  key Constraints 
CREATE TABLE CustomerSalesTransaction (
    TransactionKey INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    FullDate DATE FOREIGN KEY REFERENCES DimDates(FullDate),
    CustomerKey INT FOREIGN KEY REFERENCES DimCustomer(CustomerKey),
    SellingChannelKey INT FOREIGN KEY REFERENCES DimSellingChannel(SellingChannelKey),
    PaymentTypeKey INT FOREIGN KEY REFERENCES DimPaymentType(PaymentTypeKey),
    InvoiceNumber VARCHAR(20),
    TotalRetailPrice DECIMAL(18, 2),
    TotalCost DECIMAL(18, 2),
    CommissionRate VARCHAR(20) NOT NULL,
	Profit DECIMAL(18, 2) 
);
--ALTER TABLE CustomerSalesTransaction
--ADD Profit DECIMAL(18, 2);

----------------------------------------------------------------------------------------------------------------
-- 3.1 Implement ETL processes to populate dimension tables

-- Create Staging table for DimDates
CREATE TABLE StagingDimDates (
    DateKey INT PRIMARY KEY,
    FullDate DATE,
    DateName DATE,
    DayOfWeek INT,
    DayNameOfWeek NVARCHAR(20),
    DayOfMonth INT,
    DayOfYear INT,
    WeekdayWeekend NVARCHAR(20),
    WeekOfYear INT,
    MonthName NVARCHAR(20),
    MonthOfYear INT,
    IsLastDayOfMonth VARCHAR(3),
    CalendarQuarter INT,
    CalendarYear INT,
    CalendarYearMonth DATE,
    CalendarYearQtr NVARCHAR(20),
    FiscalMonthOfYear INT,
    FiscalQuarter INT,
    FiscalYear INT,
    FiscalYearMonth NVARCHAR(20),
    FiscalYearQtr NVARCHAR(20)
);
-- Insert Data into StagingDimDates
BULK INSERT StagingDimDates
FROM 'D:\MSc Data Analytics\Courses\CS7079-Data Warehousing and Big Data\Final Project\CS7079 CW Data Files\Generated DateTime.csv'
WITH (
    FIRSTROW = 2,  -- Skip header row if present
    FIELDTERMINATOR = ',',  -- Specify the delimiter used in your CSV file
    ROWTERMINATOR = '\n'  -- Specify the row terminator used in your CSV file

);

-- Load data into DimDates
INSERT INTO DimDates (FullDate, MonthofYear, CalendarQuarter, CalendarYear)
SELECT FullDate, MonthofYear, CalendarQuarter, CalendarYear
FROM StagingDimDates;


SELECT *FROM StagingDimDates;
SELECT *FROM DimDates;
----------------------------------------------------------------------------------------------------------------------
-- Staging table for DimCustomer
CREATE TABLE StagingDimCustomer1 (
	Name NVARCHAR(50),
    CustomerCode VARCHAR(20),
    BirthDate DATE,
    MaritalStatus NVARCHAR(10),
    Gender CHAR(10),
    PostCode NVARCHAR(10),
    City NVARCHAR(50)
);
BULK INSERT StagingDimCustomer1
FROM 'D:\MSc Data Analytics\Courses\CS7079-Data Warehousing and Big Data\Final Project\CS7079 CW Data Files\CustomerDetails-1.csv'
WITH (
    FIRSTROW = 2,  -- Skip header row if present
    FIELDTERMINATOR = ',',  -- Specify the delimiter used in your CSV file
    ROWTERMINATOR = '\n'  -- Specify the row terminator used in your CSV file

);

CREATE TABLE StagingDimCustomer2 (
	FirstName NVARCHAR(50),
	LastName NVARCHAR(50),
	Income INT
);
BULK INSERT StagingDimCustomer2
FROM 'D:\MSc Data Analytics\Courses\CS7079-Data Warehousing and Big Data\Final Project\CS7079 CW Data Files\Customer Details 2.csv'
WITH (
    FIRSTROW = 2,  -- Skip header row if present
    FIELDTERMINATOR = ',',  -- Specify the delimiter used in your CSV file
    ROWTERMINATOR = '\n'  -- Specify the row terminator used in your CSV file

);

----------Data Transformation & Cleaning-------------------------------------------------------
-- Add FirstName and LastName columns to StagingDimCustomer1
ALTER TABLE StagingDimCustomer1
ADD 
	FirstName VARCHAR(50),
    LastName VARCHAR(50);

-- Update the new columns based on the Name column
UPDATE StagingDimCustomer1
SET
    FirstName = SUBSTRING(Name, 1, CHARINDEX(' ', Name) - 1),
    LastName = SUBSTRING(Name, CHARINDEX(' ', Name) + 1, LEN(Name) - CHARINDEX(' ', Name));

-- Verify the changes
SELECT * FROM StagingDimCustomer1;

-- Update values in the Gender column
UPDATE StagingDimCustomer1
SET Gender = CASE
    WHEN Gender = 'Male' THEN 'M'
    WHEN Gender = 'Female' THEN 'F'
    ELSE Gender
END;

UPDATE StagingDimCustomer2
SET FirstName = 'Anna'
WHERE FirstName = 'Ann';
--------------------------------------------------------
CREATE TABLE StagingDimCustomer (
    Name NVARCHAR(50),
    CustomerCode VARCHAR(20),
    BirthDate DATE,
    MaritalStatus NVARCHAR(10),
    Gender CHAR(1),
    PostCode NVARCHAR(10),
    City NVARCHAR(50),
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50),
    Income INT
);

-- Move data from StagingDimCustomer1 to StagingDimCustomer
INSERT INTO StagingDimCustomer (Name, FirstName, LastName, CustomerCode, BirthDate, MaritalStatus, Gender, PostCode, City)
SELECT Name, FirstName, LastName, CustomerCode, BirthDate, MaritalStatus, Gender, PostCode, City
FROM StagingDimCustomer1;

UPDATE StagingDimCustomer
SET Income = (
    SELECT Income
    FROM StagingDimCustomer2
    WHERE StagingDimCustomer2.FirstName = StagingDimCustomer.FirstName
);

SELECT *FROM StagingDimCustomer1;
SELECT *FROM StagingDimCustomer2;
SELECT *FROM StagingDimCustomer;

----------------------------------------------------------------------------------------------------------------
-- Load data into DimCustomer
INSERT INTO DimCustomer (FirstName, LastName, CustomerCode, BirthDate, MaritalStatus, Gender, PostCode, City, Income)
SELECT FirstName, LastName, CustomerCode, BirthDate, MaritalStatus, Gender, PostCode, City, Income
FROM StagingDimCustomer;


-- To view Data in DimCustomer
Select *from StagingDimCustomer;
Select *from DimCustomer;

----------------------------------------------------------------------------------------------------------------
-- Create StagingDimSellingChannel table
CREATE TABLE StagingDimSellingChannel (
    Name NVARCHAR(50),
    Code VARCHAR(10),
    CommissionRate VARCHAR(20)
);
BULK INSERT StagingDimSellingChannel
FROM 'D:\MSc Data Analytics\Courses\CS7079-Data Warehousing and Big Data\Final Project\CS7079 CW Data Files\Selling Channels.csv'
WITH (
    FIRSTROW = 2,  -- Skip header row if present
    FIELDTERMINATOR = ',',  -- Specify the delimiter used in your CSV file
    ROWTERMINATOR = '\n'  -- Specify the row terminator used in your CSV file

);
INSERT INTO DimSellingChannel (SellingChannelName, SellingChannelCode, CommissionRate)
SELECT Name, Code, CommissionRate
FROM StagingDimSellingChannel;

SELECT * FROM StagingDimSellingChannel;
SELECT * FROM DimSellingChannel;

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Create StagingDimPaymentType table
CREATE TABLE StagingDimPaymentType (
    Name VARCHAR(50) NOT NULL,
    RetailerPaymentTypeId INT NOT NULL
);
BULK INSERT StagingDimPaymentType
FROM 'D:\MSc Data Analytics\Courses\CS7079-Data Warehousing and Big Data\Final Project\CS7079 CW Data Files\PaymentsData.csv'
WITH (
    FIRSTROW = 2,  -- Assuming the first row contains column headers
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
);
INSERT INTO DimPaymentType (PaymentTypeName, PaymentTypeID)
SELECT Name, RetailerPaymentTypeId
FROM StagingDimPaymentType;

Select *from StagingDimPaymentType;
Select *from DimPaymentType;

----------------------------------------------------------------------------------------------------------------
-- 3.2 Implement ETL processes to populate Fact table
-- Create Staging Table for Fact Table
CREATE TABLE StagingCustomerSalesTransaction (
    Date DATE,
    CustomerCode VARCHAR(20),
    SellingChannel VARCHAR(50),
    PaymentTypeID INT,
    InvoiceNumber VARCHAR(50),
    TotalRetailPrice DECIMAL(18, 2),
    TotalCost DECIMAL(18, 2)    
);
BULK INSERT StagingCustomerSalesTransaction
FROM 'D:\MSc Data Analytics\Courses\CS7079-Data Warehousing and Big Data\Final Project\CS7079 CW Data Files\CustomerSalesTransaction.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2  -- Assuming the first row contains column headers
);
select *from StagingCustomerSalesTransaction;

-- Alter StagingCustomerSalesTransaction table
ALTER TABLE StagingCustomerSalesTransaction
ADD 
	FullDate DATE,
    CustomerKey INT,
    SellingChannelKey INT,
    PaymentTypeKey INT,
	CommissionRate VARCHAR(20)

-- Add foreign key constraints
ALTER TABLE StagingCustomerSalesTransaction
ADD CONSTRAINT FK_StagingCustomerSalesTransaction_FullDate
    FOREIGN KEY (FullDate) REFERENCES DimDates(FullDate);

ALTER TABLE StagingCustomerSalesTransaction
ADD CONSTRAINT FK_StagingCustomerSalesTransaction_CustomerKey
    FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey);

ALTER TABLE StagingCustomerSalesTransaction
ADD CONSTRAINT FK_StagingCustomerSalesTransaction_SellingChannelKey
    FOREIGN KEY (SellingChannelKey) REFERENCES DimSellingChannel(SellingChannelKey);

ALTER TABLE StagingCustomerSalesTransaction
ADD CONSTRAINT FK_StagingCustomerSalesTransaction_PaymentTypeKey
    FOREIGN KEY (PaymentTypeKey) REFERENCES DimPaymentType(PaymentTypeKey);

UPDATE StagingCustomerSalesTransaction
SET CommissionRate = 
    CASE 
        WHEN ssc.Code = 'BS' THEN '0'
        WHEN ssc.Code = 'AZ' THEN '12'
        WHEN ssc.Code = 'TS' THEN '10'
        WHEN ssc.Code = 'EB' THEN '15'
        WHEN ssc.Code = 'SH' THEN '18'  
     ELSE 'DefaultCommissionRate' -- Provide a default value if none of the conditions match
    END
FROM StagingCustomerSalesTransaction scts
JOIN StagingDimSellingChannel ssc ON scts.SellingChannel = ssc.Code;

Select *from StagingCustomerSalesTransaction;

Alter Table StagingCustomerSalesTransaction
ADD Profit DECIMAL(18, 2) 

UPDATE StagingCustomerSalesTransaction
SET Profit = TotalRetailPrice - TotalCost;

-- Update FullDate Column										
UPDATE StagingCustomerSalesTransaction
SET FullDate = (
    SELECT FullDate
    FROM StagingDimDates
    WHERE StagingDimDates.FullDate = StagingCustomerSalesTransaction.Date
);
-- Update CustomerKey Column
UPDATE StagingCustomerSalesTransaction
SET CustomerKey = (
    SELECT CustomerKey
    FROM DimCustomer
    WHERE DimCustomer.CustomerCode = StagingCustomerSalesTransaction.CustomerCode
);
-- Update SellingChannelKey Column
UPDATE StagingCustomerSalesTransaction
SET SellingChannelKey = (
    SELECT SellingChannelKey
    FROM DimSellingChannel
    WHERE DimSellingChannel.SellingChannelCode = StagingCustomerSalesTransaction.SellingChannel
);
-- Update PaymentTypeKey Column
UPDATE StagingCustomerSalesTransaction
SET PaymentTypeKey = (
    SELECT PaymentTypeKey
    FROM DimPaymentType
    WHERE DimPaymentType.PaymentTypeID = StagingCustomerSalesTransaction.PaymentTypeID
); Select *from StagingCustomerSalesTransaction;

-------------------------------------------------------------------------------------------------

-- Insert Data from StagingCustomerSalesTransaction to CustomerSalesTransaction Fact Table
INSERT INTO CustomerSalesTransaction (FullDate, CustomerKey, SellingChannelKey, PaymentTypeKey, InvoiceNumber, TotalRetailPrice, TotalCost, CommissionRate)
SELECT FullDate, CustomerKey, SellingChannelKey, PaymentTypeKey, InvoiceNumber, TotalRetailPrice, TotalCost, CommissionRate
FROM StagingCustomerSalesTransaction;

UPDATE CustomerSalesTransaction
SET Profit = TotalRetailPrice - TotalCost;

Select *From StagingCustomerSalesTransaction;
Select *From CustomerSalesTransaction;

-- Drop staging tables
DROP TABLE StagingDimDates;
DROP TABLE StagingDimCustomer1;
DROP TABLE StagingDimCustomer2;
DROP TABLE StagingDimCustomer;
DROP TABLE StagingDimSellingChannel;
DROP TABLE StagingDimPaymentType;
DROP TABLE StagingCustomerSalesTransaction;

-------------------------------------------------------------------------------------------------------
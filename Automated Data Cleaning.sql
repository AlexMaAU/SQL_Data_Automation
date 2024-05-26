-- Create data cleaning procedure
DELIMITER $$
DROP PROCEDURE IF EXISTS Copy_And_Clean_Data;

CREATE PROCEDURE Copy_And_Clean_Data()
BEGIN
	-- Create a working table
	CREATE TABLE IF NOT EXISTS `us_household_income_clean` (
	  `row_id` int DEFAULT NULL,
	  `id` int DEFAULT NULL,
	  `State_Code` int DEFAULT NULL,
	  `State_Name` text,
	  `State_ab` text,
	  `County` text,
	  `City` text,
	  `Place` text,
	  `Type` text,
	  `Primary` text,
	  `Zip_Code` int DEFAULT NULL,
	  `Area_Code` int DEFAULT NULL,
	  `ALand` int DEFAULT NULL,
	  `AWater` int DEFAULT NULL,
	  `Lat` double DEFAULT NULL,
	  `Lon` double DEFAULT NULL,
	  `TimeStamp` TIMESTAMP DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    
    -- Copy Data to new table
    INSERT INTO us_household_income_clean
    SELECT *, CURRENT_TIMESTAMP
    FROM ushouseholdincome;
    
	-- Remove Duplicates
	DELETE FROM us_household_income_clean 
	WHERE 
		row_id IN (
		SELECT row_id
	FROM (
		SELECT row_id, id,
			ROW_NUMBER() OVER (
				PARTITION BY id, `TimeStamp`
				ORDER BY id) AS row_num
		FROM 
			us_household_income_clean
	) duplicates
	WHERE 
		row_num > 1
	);

	-- Fixing some data quality issues by fixing typos and general standardization
	UPDATE us_household_income_clean
	SET State_Name = 'Georgia'
	WHERE State_Name = 'georia';

	UPDATE us_household_income_clean
	SET County = UPPER(County);

	UPDATE us_household_income_clean
	SET City = UPPER(City);

	UPDATE us_household_income_clean
	SET Place = UPPER(Place);

	UPDATE us_household_income_clean
	SET State_Name = UPPER(State_Name);

	UPDATE us_household_income_clean
	SET `Type` = 'CDP'
	WHERE `Type` = 'CPD';

	UPDATE us_household_income_clean
	SET `Type` = 'Borough'
	WHERE `Type` = 'Boroughs';

END $$
DELIMITER ;

-- create event
DELIMITER $$
CREATE EVENT run_data_cleaning
ON SCHEDULE EVERY 1 MINUTE
DO
BEGIN
	CALL Copy_And_Clean_Data();
END $$
DELIMITER ;

-- create Trigger
DELIMITER $$
CREATE TRIGGER Transfer_Clean_Data
	AFTER INSERT ON ushouseholdincome
    FOR EACH ROW
BEGIN
	CALL Copy_And_Clean_Data();
END $$
DELIMITER ;

-- OPEN event_scheduler
SET GLOBAL event_scheduler = 'ON';
    
-- test
SHOW VARIABLES LIKE 'event%';
    
SHOW EVENTS;

DROP EVENT run_data_cleaning;
    
SELECT *
FROM us_household_income_clean;
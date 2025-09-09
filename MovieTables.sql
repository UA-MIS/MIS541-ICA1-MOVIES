use [541-Movie-Database]
Go


CREATE TABLE Director (
	DirectorID INT IDENTITY(1,1) PRIMARY KEY,
	Name NVARCHAR(100) NOT NULL
);

CREATE TABLE Studio (
	StudioID INT IDENTITY(1,1) PRIMARY KEY,
	Name NVARCHAR(100) NOT NULL
);

CREATE TABLE Movies (
	MovieID INT IDENTITY(1,1) PRIMARY KEY,
	Title NVARCHAR(200) NOT NULL,
	Plot NVARCHAR(MAX),
	RottenTomatoes TINYINT NULL,
	Factoid NVARCHAR(500),
	DirectorID INT,
	StudioID INT
);

CREATE TABLE ErrorMovies (
	Title NVARCHAR(200),
	Director NVARCHAR(100),
	Studio NVARCHAR(100)
);

ALTER TABLE Movies
ADD CONSTRAINT FK_Movies_Director FOREIGN KEY (DirectorID)
REFERENCES Director(DirectorID);

ALTER TABLE Movies
ADD CONSTRAINT FK_Movies_Studio FOREIGN KEY (StudioID)
REFERENCES Studio(StudioID);



-- query i got from chat to reset the ids and the data in the tables for testing purposes
BEGIN TRY
    BEGIN TRAN;

    -- Temporarily disable FKs on Movies
    ALTER TABLE dbo.Movies NOCHECK CONSTRAINT ALL;

    -- Delete child tables first
    DELETE FROM dbo.Movies;
    DELETE FROM dbo.ErrorMovies;  -- no identity column here

    -- Then lookup tables
    DELETE FROM dbo.Director;
    DELETE FROM dbo.Studio;

    -- Reseed identity tables so next insert = 1
    DBCC CHECKIDENT ('dbo.Movies',   RESEED, 0);
    DBCC CHECKIDENT ('dbo.Director', RESEED, 0);
    DBCC CHECKIDENT ('dbo.Studio',   RESEED, 0);

    -- Re-enable and validate FKs
    ALTER TABLE dbo.Movies WITH CHECK CHECK CONSTRAINT ALL;

    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRAN;
    THROW;
END CATCH;
GO


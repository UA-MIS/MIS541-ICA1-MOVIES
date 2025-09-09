CREATE DATABASE MoviesDB;
GO

USE MoviesDB;
GO

--Studio table
CREATE TABLE Studio (
    StudioID INT PRIMARY KEY IDENTITY(1,1),
    Name NVARCHAR(100) NOT NULL
);
GO

-- Director table
CREATE TABLE Director (
    DirectorID INT PRIMARY KEY IDENTITY(1,1),
    Name NVARCHAR(100) NOT NULL,
    StudioID INT NOT NULL,
    FOREIGN KEY (StudioID) REFERENCES Studio(StudioID)
);
GO

-- Movies table
CREATE TABLE Movies (
    MovieID INT PRIMARY KEY IDENTITY(1,1),
    Title NVARCHAR(200) NOT NULL,
    Plot NVARCHAR(MAX),
    Rating NVARCHAR(MAX),
    Factoid NVARCHAR(MAX),
    DirectorID INT NOT NULL,
    FOREIGN KEY (DirectorID) REFERENCES Director(DirectorID)
);
GO

CREATE TABLE Error_Movies (
    ErrorID INT PRIMARY KEY IDENTITY(1,1),
    Title NVARCHAR(200) NOT NULL,
);
GO

USE MovieDB;
GO



CREATE TABLE Studio (
    StudioID INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(255) NOT NULL
);




CREATE TABLE Director (
    DirectorID INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(255) NOT NULL
);



CREATE TABLE Movie (
    MovieID INT IDENTITY(1,1) PRIMARY KEY,
    Title NVARCHAR(255) NOT NULL,
    Plot NVARCHAR(MAX),
    Rating NVARCHAR(10), -- e.g. PG, R, PG-13
    Factoid NVARCHAR(MAX),
    DirectorID INT NOT NULL,
    StudioID INT NOT NULL,
    CONSTRAINT FK_Movie_Director FOREIGN KEY (DirectorID)
        REFERENCES Director(DirectorID),
    CONSTRAINT FK_Movie_Studio FOREIGN KEY (StudioID)
        REFERENCES Studio(StudioID)
);

CREATE TABLE ErrorMovies (
    ErrorMovieID INT IDENTITY(1,1) PRIMARY KEY,
    Title NVARCHAR(255) NOT NULL,
    Plot NVARCHAR(MAX) NULL,
    Rating NVARCHAR(50) NULL
);


DELETE FROM Movie;

DELETE FROM Director;
DELETE FROM Studio;

DELETE FROM ErrorMovies;



use assignment1;

CREATE TABLE Studios (
    studio_id INT IDENTITY(1,1) PRIMARY KEY,
    studio_name NVARCHAR(255) NOT NULL
);

CREATE TABLE Directors (
    director_id INT IDENTITY(1,1) PRIMARY KEY,
    director_name NVARCHAR(255) NOT NULL,
    studio_id INT NOT NULL,
    FOREIGN KEY (studio_id) REFERENCES Studios(studio_id)
);

CREATE TABLE Movies (
    movie_id INT IDENTITY(1,1) PRIMARY KEY,
    title NVARCHAR(255) NOT NULL,
    plot NVARCHAR(MAX),
    rating INT,
    director_id INT NOT NULL,
    factoid NVARCHAR(255),
    FOREIGN KEY (director_id) REFERENCES Directors(director_id)
);

CREATE TABLE ErrorMovies (
    errored_movie_id INT IDENTITY(1,1) PRIMARY KEY,
    title NVARCHAR(255) NOT NULL
);
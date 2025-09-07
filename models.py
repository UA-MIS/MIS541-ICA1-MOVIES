
class Movie:
    def __init__(self, title, director=None, studio=None, plot=None, rating=None, factoid=None):
        self.title = title
        self.director = director
        self.studio = studio
        self.plot = plot
        self.rating = rating
        self.factoid = factoid

    def __repr__(self):
        return f"<Movie title={self.title} director={self.director} studio={self.studio}>"

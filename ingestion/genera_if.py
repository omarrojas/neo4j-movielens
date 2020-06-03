import csv
import pandas as pd 

N_MOVIES = 58099
N_RATINGS = 27753445
N_TAGS = 1108998
N_LINKS = 58099

def main():
    '''
    print("Step 1 out of 4: generating movie file and movie_genre file")
    generaMoviesAndGRelations()
    print("Step 2 out of 4: generating genre file")
    generaGenres()
    print("Step 3 out of 4: generating users and user_ratings files")
    generaUsersRatings()
    '''
    generaNames()

def generaMoviesAndGRelations():
    import csv

    in_path = 'data_lg/movies.csv'
    out_path = 'data_imp_lg/movies.csv'
    out_path2 = 'data_imp_lg/movies_genre.csv'

    with open(in_path, 'r', newline='', encoding='utf-8') as inputFile, open(out_path, 'w', newline='', encoding='utf-8') as writerFile, open(out_path2, 'w', newline='', encoding='utf-8') as writerFile2:
        readCSV = csv.reader(inputFile, delimiter=',')
        writeCSV = csv.writer(writerFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writeCSV2 = csv.writer(writerFile2, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writeCSV.writerow(['movieId', 'title', 'year'])  # write header
        writeCSV2.writerow(['movieId', 'name'])  # write header 2
        next(readCSV, None)  # skip header
        for i, row in enumerate(readCSV):
            movieData = parseRowMovie(row)
            id = movieData[0]
            title = movieData[1]
            year = movieData[2]
            movieGenres = movieData[3]
            writeCSV.writerow([id, title, year])

            for movieGenre in movieGenres:
                writeCSV2.writerow([id, movieGenre])

            if (i % 100 == 0):
                print(f"{i}/{N_MOVIES} Movie nodes created")

            # break after N_MOVIES movies

            if i >= N_MOVIES:
                break

def generaUsersRatings():
    import csv

    in_path = 'data_lg/ratings.csv'
    out_path = 'data_imp_lg/users.csv'
    out_path2 = 'data_imp_lg/user_rating.csv'

    with open(in_path, 'r', newline='', encoding='utf-8') as inputFile, open(out_path, 'w', newline='', encoding='utf-8') as writerFile, open(out_path2, 'w', newline='', encoding='utf-8') as writerFile2:
        readCSV = csv.reader(inputFile, delimiter=',')
        writeCSV = csv.writer(writerFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writeCSV2 = csv.writer(writerFile2, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writeCSV.writerow(['userId'])  # write header
        writeCSV2.writerow(['userId', 'movieId', 'rating', 'timestamp'])  # write header 2
        next(readCSV, None)  # skip header
        for i, row in enumerate(readCSV):
            ratingData = parseRowRatingRelationships(row)
            userId = ratingData[0]
            movieId = ratingData[1]
            rating = ratingData[2]
            timestamp = ratingData[3]
            writeCSV.writerow([userId])
            writeCSV2.writerow([userId, movieId, rating, timestamp])

            if (i % 100 == 0):
                print(f"{i}/{N_RATINGS} Ratings nodes created")

            # break after N_RATINGS ratings

            if i >= N_RATINGS:
                break

def generaGenres():
    out_path = 'data_imp_lg/genres.csv'
    allGenres = ["Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
                 "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical",
                 "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]
    with open(out_path, 'w', newline='', encoding='utf-8') as writerFile:
        writeCSV = csv.writer(writerFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writeCSV.writerow(['name'])  # write header

        for genre in allGenres:
            writeCSV.writerow([genre])

def parseRowMovie(row):
        id = row[0]
        year = row[1][-5:-1]
        title = row[1][:-7]
        movieGenres = row[2].split("|")

        return (id, title, year, movieGenres)

def parseRowRatingRelationships(row):
    userId = "User " + row[0]
    movieId = row[1]
    rating = float(row[2])
    timestamp = row[3]

    return (userId, movieId, rating, timestamp)

def generaNames():
    in_path = 'data_lg/name.basics.tsv'
    out_path = 'data_imp_lg/names.csv'
    data = pd.read_csv(in_path, sep='\t', encoding='utf-8') 
    # Preview the first 5 lines of the loaded data 
    print (data.head())

if __name__ == '__main__':
    main()


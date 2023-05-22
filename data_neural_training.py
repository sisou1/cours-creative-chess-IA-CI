import xml.etree.ElementTree as ET
import mysql.connector
import chess.pgn
import csv
from db_config import db_config

# Connexion à la base de données
creativechessai_db = mysql.connector.connect(**db_config)

# Création d'un curseur pour exécuter des requêtes
cursor = creativechessai_db.cursor()

# Vérification de l'existence de la table 'games'
cursor.execute("SHOW TABLES LIKE 'games'")
result = cursor.fetchone()
if result:
    print("The 'games' table exists.")
else:
    print("The 'games' table does not exist.")

    # You may want to create the table here using a CREATE TABLE statement
    # ...

# Open the PGN file
pgn = open('ChessDB/chessdb.pgn')

# Count the total number of games in the PGN file
total_games_pgn = 0
for line in pgn:
    if line.startswith("[Event"):
        total_games_pgn += 1

# Reset the file pointer to the beginning of the file
pgn.seek(0)

# Create a set to store unique games
unique_games = set()

# Browse through the games in the PGN file one by one
while True:
    # Read a game
    game = chess.pgn.read_game(pgn)
    if game is None:
        break

    # Check if the game already exists in the unique set
    if game in unique_games:
        continue  # Skip the game if it already exists

    # Add the game to the unique set
    unique_games.add(game)

# Prepare the SQL query for insertion
insert_query = "INSERT INTO games (event, site, date, round, white, black, result, moves) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"

# Initialize a list to store the data for each game
games_data = []

# Iterate over the unique games
for game in unique_games:
    # Check if the game already exists in the database
    cursor.execute(
        "SELECT COUNT(*) FROM games WHERE event = %s AND site = %s AND date = %s AND round = %s AND white = %s AND black = %s AND result = %s AND moves = %s",
        (game.headers.get('Event'), game.headers.get('Site'), game.headers.get('Date'), game.headers.get('Round'),
         game.headers.get('White'), game.headers.get('Black'), game.headers.get('Result'), str(game.mainline_moves())))
    if cursor.fetchone()[0] > 0:
        continue  # Skip the game if it already exists

    # Add the game data to the list
    game_data = (
        game.headers.get('Event'), game.headers.get('Site'), game.headers.get('Date'), game.headers.get('Round'),
        game.headers.get('White'), game.headers.get('Black'), game.headers.get('Result'),
        str(game.mainline_moves()))
    games_data.append(game_data)

    # Check if the batch size is reached
    if len(games_data) >= batch_size:
        try:
            cursor.executemany(insert_query, games_data)
            creativechessai_db.commit()
            batch_count += len(games_data)
            print(f"{batch_count} games inserted into the database.")
        except mysql.connector.Error as err:
            print(f"Error during insertion: {err}")
            creativechessai_db.rollback()
        games_data = []

# Insert any remaining games in the last batch
if games_data:
    try:
        cursor.executemany(insert_query, games_data)
        creativechessai_db.commit()
        batch_count += len(games_data)
        print(f"{batch_count} games inserted into the database.")
    except mysql.connector.Error as err:
        print(f"Error during insertion: {err}")
        creativechessai_db.rollback()

# Count the total number of games in the 'games' table
cursor.execute("SELECT COUNT(*) FROM games")
total_games_db = cursor.fetchone()[0]

# Compare the total number of games
print(f"Total games in PGN file: {total_games_pgn}")
print(f"Total games in database: {total_games_db}")

if total_games_db == total_games_pgn:
    print("All games from the PGN file have been inserted into the database.")
else:
    print("Some games from the PGN file may not have been inserted into the database.")

    # Requête pour supprimer les doublons de la base de données
    delete_duplicates_query = """
        DELETE t1 FROM games t1
        INNER JOIN games t2
        WHERE 
            t1.id > t2.id
            AND t1.event = t2.event
            AND t1.site = t2.site
            AND t1.date = t2.date
            AND t1.round = t2.round
            AND t1.white = t2.white
            AND t1.black = t2.black
            AND t1.result = t2.result
            AND t1.moves = t2.moves
    """

    # Exécuter la requête de suppression des doublons
    cursor.execute(delete_duplicates_query)
    creativechessai_db.commit()

    # Count the total number of games in the 'games' table after removing duplicates
    cursor.execute("SELECT COUNT(*) FROM games")
    total_games_db = cursor.fetchone()[0]

    print(f"Total games in database after removing duplicates: {total_games_db}")

# Close the PGN file
pgn.close()

# Close the database connection
cursor.close()
creativechessai_db.close()
print("The script is finished.")

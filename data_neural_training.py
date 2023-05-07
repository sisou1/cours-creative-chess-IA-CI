# By PEREZ MOTA Javier

# Preparing data for neural network training
import mysql.connector
import chess.pgn
from db_config import db_config

# To connect to the database
creativechessai_db = mysql.connector.connect(**db_config)

# Open th PGN file
pgn = open('ChessDB/Databases.pgn')

# Get ID of the last inserted game
cursor = creativechessai_db.cursor()
cursor.execute("SELECT MAX(id) FROM games")
last_id = cursor.fetchone()[0]

# Browse throught the games in the PGN file one by one
while True:
    # Read a game
    game = chess.pgn.read_game(pgn)
    if game is None:
        break
    # Check if the game has already been inserted
    if game.headers.get('Event') is not None and int(game.headers.get('Event')) <= last_id:
        continue
    # Inser the game into database
    cursor = creativechessai_db.cursor()
    sql = "INSERT INTO games (event, site, date, round, white, black, result, moves) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
    val = (game.headers.get('Event'), game.headers.get('Site'), game.headers.get('Date'), game.headers.get('Round'),
           game.headers.get('White'), game.headers.get('Black'), game.headers.get('Result'),
           str(game.mainline_moves()))
    cursor.execute(sql, val)
    creativechessai_db.commit()

# Close files and the database connection
pgn.close()
creativechessai_db.close()
print("The script is finished.")

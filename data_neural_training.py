# By PEREZ MOTA Javier

# Preparing data for neural network training
import mysql.connector
import chess.pgn
from db_config import db_config

# To connect to the database
creativechessai_db = mysql.connector.connect(**db_config)

# Open the PGN file
pgn = open('ChessDB/Databases.pgn')

# Browse through the games in the PGN file one by one
while True:
    # Read a game
    game = chess.pgn.read_game(pgn)
    if game is None:
        break
    # Initialize the board to the starting position of the game
    board = game.board()

    # Browse through the moves of the game
    for move in game.mainline_moves():

        # Check if the current position is an opening position
        if board.is_valid() and board.ply() <= 10:
            # Add the opening position to the database
            position = board.fen()
            cursor = creativechessai_db.cursor()
            sql = "INSERT INTO openings (position) VALUES (%s)"
            val = (position,)
            cursor.execute(sql, val)
            creativechessai_db.commit()

        # Play the move on the board
        board.push(move)

# Close the files and the database connection
pgn.close()
creativechessai_db.close()
print("Le script est terminé.")

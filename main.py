import psycopg2
from psycopg2 import sql
import csv

# Credentials
HOST = 'localhost'
PORT = '5432'
DB_NAME = 'proyecto1'
USER = 'postgres'
PASSWORD = 'cas21562'

def separator():
    print("=================================================================================================")

def connect_to_database():
    try:
        conn = psycopg2.connect(f"host={HOST} port={PORT} user={USER} password={PASSWORD} dbname={DB_NAME} ")
        print("Connection successful")
        separator()
    except:
        print("Connection failed")
        separator()
    return conn

def success_table_creation(source):
    print(f"The following table has been created successfully: {source}")

def success_table_truncation(source):
    print(f"The following table has been truncated successfully: {source}")

def success_data_upload(source):
    print(f"The following data has been uploaded successfully: {source}")

def create_all_tables():
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS country (
            id INTEGER PRIMARY KEY,
            country_name VARCHAR(50) NOT NULL);
    ''')
    success_table_creation('country')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS league (
            id INTEGER PRIMARY KEY,
            country_id INTEGER NOT NULL,
            name VARCHAR(50) NOT NULL,
            FOREIGN KEY (country_id) REFERENCES country (id));
    ''')
    success_table_creation('league')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS team (
            id INTEGER PRIMARY KEY,
            team_api_id INTEGER NOT NULL,
            team_fifa_api_id INTEGER,
            team_long_name VARCHAR(50) NOT NULL,
            team_short_name VARCHAR(50) NOT NULL);
    ''')
    success_table_creation('team')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS team_attributes (
            id INTEGER PRIMARY KEY,
            team_fifa_api_id INTEGER NOT NULL,
            team_api_id INTEGER NOT NULL,
            date DATE NOT NULL,
            buildUpPlaySpeed INTEGER,
            buildUpPlaySpeedClass VARCHAR(50),
            buildUpPlayDribbling INTEGER,
            buildUpPlayDribblingClass VARCHAR(50),
            buildUpPlayPassing INTEGER,
            buildUpPlayPassingClass VARCHAR(50),
            buildUpPlayPositioningClass VARCHAR(50),
            chanceCreationPassing INTEGER,
            chanceCreationPassingClass VARCHAR(50),
            chanceCreationCrossing INTEGER,
            chanceCreationCrossingClass VARCHAR(50),
            chanceCreationShooting INTEGER,
            chanceCreationShootingClass VARCHAR(50),
            chanceCreationPositioningClass VARCHAR(50),
            defencePressure INTEGER,
            defencePressureClass VARCHAR(50),
            defenceAggression INTEGER,
            defenceAggressionClass VARCHAR(50),
            defenceTeamWidth INTEGER,
            defenceTeamWidthClass VARCHAR(50),
            defenceDefenderLineClass VARCHAR(50));
    ''')
    success_table_creation('team_attributes')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player (
            id INTEGER PRIMARY KEY,
            player_api_id INTEGER NOT NULL,
            player_name VARCHAR(50) NOT NULL,
            player_fifa_api_id INTEGER NOT NULL,
            birthday DATE NOT NULL,
            height FLOAT NOT NULL,
            weight INTEGER NOT NULL);
    ''')
    success_table_creation('player')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_attributes (
            id INTEGER PRIMARY KEY,
            player_fifa_api_id INTEGER NOT NULL,
            player_api_id INTEGER NOT NULL,
            date DATE NOT NULL,
            overall_rating INTEGER,
            potential INTEGER,
            preferred_foot VARCHAR(50),
            attacking_work_rate VARCHAR(50),
            defensive_work_rate VARCHAR(50),
            crossing INTEGER,
            finishing INTEGER,
            heading_accuracy INTEGER,
            short_passing INTEGER,
            volleys INTEGER,
            dribbling INTEGER,
            curve INTEGER,
            free_kick_accuracy INTEGER,
            long_passing INTEGER,
            ball_control INTEGER,
            acceleration INTEGER,
            sprint_speed INTEGER,
            agility INTEGER,
            reactions INTEGER,
            balance INTEGER,
            shot_power INTEGER,
            jumping INTEGER,
            stamina INTEGER,
            strength INTEGER,
            long_shots INTEGER,
            aggression INTEGER,
            interceptions INTEGER,
            positioning INTEGER,
            vision INTEGER,
            penalties INTEGER,
            marking INTEGER,
            standing_tackle INTEGER,
            sliding_tackle INTEGER,
            gk_diving INTEGER,
            gk_handling INTEGER,
            gk_kicking INTEGER,
            gk_positioning INTEGER,
            gk_reflexes INTEGER);
    ''')
    success_table_creation('player_attributes')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS match (
            id INTEGER PRIMARY KEY,
            country_id INTEGER NOT NULL,
            league_id INTEGER NOT NULL,
            season VARCHAR(50) NOT NULL,
            stage INTEGER NOT NULL,
            date DATE NOT NULL,
            match_api_id INTEGER NOT NULL,
            home_team_api_id INTEGER NOT NULL,
            away_team_api_id INTEGER NOT NULL,
            home_team_goal INTEGER NOT NULL,
            away_team_goal INTEGER NOT NULL,
            home_player_X1 INTEGER,
            home_player_X2 INTEGER,
            home_player_X3 INTEGER,
            home_player_X4 INTEGER,
            home_player_X5 INTEGER,
            home_player_X6 INTEGER,
            home_player_X7 INTEGER,
            home_player_X8 INTEGER,
            home_player_X9 INTEGER,
            home_player_X10 INTEGER,
            home_player_X11 INTEGER,
            away_player_X1 INTEGER,
            away_player_X2 INTEGER,
            away_player_X3 INTEGER,
            away_player_X4 INTEGER,
            away_player_X5 INTEGER,
            away_player_X6 INTEGER,
            away_player_X7 INTEGER,
            away_player_X8 INTEGER,
            away_player_X9 INTEGER,
            away_player_X10 INTEGER,
            away_player_X11 INTEGER,
            home_player_Y1 INTEGER,
            home_player_Y2 INTEGER,
            home_player_Y3 INTEGER,
            home_player_Y4 INTEGER,
            home_player_Y5 INTEGER,
            home_player_Y6 INTEGER,
            home_player_Y7 INTEGER,
            home_player_Y8 INTEGER,
            home_player_Y9 INTEGER,
            home_player_Y10 INTEGER,
            home_player_Y11 INTEGER,
            away_player_Y1 INTEGER,
            away_player_Y2 INTEGER,
            away_player_Y3 INTEGER,
            away_player_Y4 INTEGER,
            away_player_Y5 INTEGER,
            away_player_Y6 INTEGER,
            away_player_Y7 INTEGER,
            away_player_Y8 INTEGER,
            away_player_Y9 INTEGER,
            away_player_Y10 INTEGER,
            away_player_Y11 INTEGER,
            home_player_1 INTEGER,
            home_player_2 INTEGER,
            home_player_3 INTEGER,
            home_player_4 INTEGER,
            home_player_5 INTEGER,
            home_player_6 INTEGER,
            home_player_7 INTEGER,
            home_player_8 INTEGER,
            home_player_9 INTEGER,
            home_player_10 INTEGER,
            home_player_11 INTEGER,
            away_player_1 INTEGER,
            away_player_2 INTEGER,
            away_player_3 INTEGER,
            away_player_4 INTEGER,
            away_player_5 INTEGER,
            away_player_6 INTEGER,
            away_player_7 INTEGER,
            away_player_8 INTEGER,
            away_player_9 INTEGER,
            away_player_10 INTEGER,
            away_player_11 INTEGER,
            goal INTEGER,
            shoton INTEGER,
            shotoff INTEGER,
            foulcommit INTEGER,
            card INTEGER,
            "cross" INTEGER,
            corner INTEGER,
            possession INTEGER,
            B365H FLOAT,
            B365D FLOAT,
            B365A FLOAT,
            BWH FLOAT,
            BWD FLOAT,
            BWA FLOAT,
            IWH FLOAT,
            IWD FLOAT,
            IWA FLOAT,
            LBH FLOAT,
            LBD FLOAT,
            LBA FLOAT,
            PSH FLOAT,
            PSD FLOAT,
            PSA FLOAT,
            WHH FLOAT,
            WHD FLOAT,
            WHA FLOAT,
            SJH FLOAT,
            SJD FLOAT,
            SJA FLOAT,
            VCH FLOAT,
            VCD FLOAT,
            VCA FLOAT,
            GBH FLOAT,
            GBD FLOAT,
            GBA FLOAT,
            BSH FLOAT,
            BSD FLOAT,
            BSA FLOAT);
    ''')
    success_table_creation('match')

    separator()

def truncate_all_tables():
    cursor.execute('''
        TRUNCATE TABLE country CASCADE;
    ''')
    success_table_truncation('country')

    cursor.execute('''
        TRUNCATE TABLE league CASCADE;
    ''')
    success_table_truncation('league')

    cursor.execute('''
        TRUNCATE TABLE team CASCADE;
    ''')
    success_table_truncation('team')

    cursor.execute('''
        TRUNCATE TABLE team_attributes CASCADE;
    ''')
    success_table_truncation('team_attributes')

    cursor.execute('''
        TRUNCATE TABLE player CASCADE;
        ''')
    success_table_truncation('player')

    cursor.execute(''' 
        TRUNCATE TABLE player_attributes CASCADE;
    ''')
    success_table_truncation('player_attributes')

    cursor.execute('''
        TRUNCATE TABLE match CASCADE;
    ''')
    success_table_truncation('match')
    separator()

def upload_all_tables_data():
    create_all_tables()
    truncate_all_tables()
    with open('data/Country.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row.
        for row in reader:
            cursor.execute(f"INSERT INTO country VALUES {row[0], row[1]}")
    success_data_upload('country')

    with open('data/League.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute(f"INSERT INTO league VALUES {row[0], row[1], row[2]}")
    success_data_upload('league')

    with open('data/Team.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if row[2] == '':
                row[2] = -1
            query = sql.SQL("INSERT INTO team VALUES ({}, {}, {}, {}, {})").format(
                sql.Literal(row[0]),
                sql.Literal(row[1]),
                sql.Literal(row[2]),
                sql.Literal(row[3]),
                sql.Literal(row[4])
            )
            cursor.execute(query)
    success_data_upload('team')

    with open('data/Team_Attributes.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if row[6] == '':
                row[6] = -1
            cursor.execute(f"INSERT INTO team_attributes VALUES {row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23]}")
    success_data_upload('team_attributes')

    with open('data/Player.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            query = sql.SQL("INSERT INTO player VALUES ({}, {}, {}, {}, {}, {}, {})").format(
                sql.Literal(row[0]),
                sql.Literal(row[1]),
                sql.Literal(row[2]),
                sql.Literal(row[3]),
                sql.Literal(row[4]),
                sql.Literal(row[5]),
                sql.Literal(row[6])
            )
            cursor.execute(query)
    success_data_upload('player')

    with open('data/Player_Attributes.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            values = ""
            for i in range(len(row)):
                # Checks if the last value should be null
                if row[i] == '' and i == len(row) - 1:
                    values += "NULL"
                # Checks if the value should be null
                elif row[i] == '':
                    values += "NULL,"
                # Checks if the value should be a string
                elif i == 3 or i in range(6, 9):
                    values += "'" + row[i] + "',"
                # Checks if it's the last value
                elif i == len(row) - 1:
                    values += row[i]
                # Adds a comma to the end of the value
                else:
                    values += row[i] + ","
            cursor.execute(f"INSERT INTO player_attributes VALUES ({values})")
    success_data_upload('player_attributes')

    with open('data/Match.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            values = ""
            for i in range(len(row)):
                # Checks if the last value should be null
                if row[i] == '' and i == len(row) - 1:
                    values += "-1"
                # Checks the season column
                elif i == 3:
                    values += "'" + row[i] + "',"
                # Checks if the value should be null
                elif row[i] == '':
                    values += "-1,"
                # Checks if the value should be a string
                elif i == 5:
                    values += "'" + row[i] + "',"
                # Checks if it's the last value
                elif i == len(row) - 1:
                    values += row[i]
                # Removes bad data
                elif i in range(77, 86):
                    values += "NULL,"
                # Adds a comma to the end of the value
                else:
                    values += row[i] + ","
            cursor.execute(f"INSERT INTO match VALUES ({values})")
    success_data_upload('match')

    connection.commit()
    connection.close()
    separator()

# Connect to the database
connection = connect_to_database()
cursor = connection.cursor()
upload_all_tables_data()

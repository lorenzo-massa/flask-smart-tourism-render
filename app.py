import pandas as pd
from flask import *
import sqlite3
from monument_recommendation import Recommendation
import csv
import time
from apscheduler.schedulers.background import BackgroundScheduler
from flask_cors import CORS
import sys

app = Flask(__name__)
CORS(app)


def table_creation():
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS user (id integer PRIMARY KEY, personal_id text NOT NULL, firstname text NOT NULL, lastname text NOT NULL)")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS monument (id integer PRIMARY KEY, name text NOT NULL, description text NOT NULL, category text NOT NULL, image text NOT NULL)")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS interaction (user_id integer NOT NULL,monument_id integer NOT NULL,PRIMARY KEY (user_id, monument_id),FOREIGN KEY (user_id) REFERENCES users(id),FOREIGN KEY (monument_id) REFERENCES monuments(id))")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS recommendation (user_id integer PRIMARY KEY, r1 text NOT NULL, r2 text NOT NULL, r3 text NOT NULL, FOREIGN KEY (user_id) REFERENCES users(id))")

    cursor.connection.close()


def db_connection():
    conn = sqlite3.connect('data/database.sqlite')
    return conn


def update_recommendation_db():
    # Qui puoi inserire la logica per accedere al database e ottenere la lista degli utenti
    # per ogni utente, chiamare la funzione get_recommendation() e aggiornare i dati nel database
    print("inizio l'update")
    conn = db_connection()
    cursor = conn.cursor()
    create_csv(cursor)
    recommender = Recommendation('data/interaction.csv', 'data/monument.csv')

    cursor.execute('BEGIN TRANSACTION')
    cursor.execute("SELECT id FROM user")
    ids = [row[0] for row in cursor.fetchall()]
    for i in ids:
        data = recommender.recommendation(i)
        cursor.execute(
            'INSERT INTO recommendation (user_id, r1, r2, r3) VALUES (?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET r1 = ?, r2 = ?, r3 = ?',
            (i, data[0], data[1], data[2], data[0], data[1], data[2]))
    conn.commit()
    conn.close()
    print("fine update")
    pass


scheduler = BackgroundScheduler()
scheduler.add_job(update_recommendation_db, 'interval', minutes=2)
scheduler.start()


def create_csv(cursor):
    cursor.execute("SELECT * FROM interaction")
    interactions = [[row[0], row[1]] for row in cursor.fetchall()]
    interaction_head = ["user_id", "monument_id"]
    with open('data/interaction.csv', mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(interaction_head)
        # Write each data row
        for row in interactions:
            writer.writerow(row)
            time.sleep(0.1)
    cursor.execute("SELECT * FROM monument")
    monuments = [[row[0], row[1], row[2], row[3]] for row in cursor.fetchall()]
    monuments_head = ["mon_id", "name", "description", "category"]
    with open('data/monument.csv', mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(monuments_head)
        # Write each data row
        for row in monuments:
            writer.writerow(row)
            time.sleep(0.1)


@app.route('/', methods=['GET'])
def index():
    print("table_creation", file=sys.stderr)
    table_creation()

    return 'Welcome to the recommendation API!'


@app.route('/user', methods=['GET', 'POST'])
def user():
    conn = db_connection()
    cursor = conn.cursor()

    if request.method == 'GET':
        cursor = conn.execute("SELECT * FROM user")
        users = [dict(id=row[0], pers_id=row[1], firstname=row[2], lastname=row[3]) for row in cursor.fetchall()]
        if users is not None:
            return jsonify(users)

    if request.method == 'POST':
        new_pers_id = request.form['personal_id']
        new_firstname = request.form['firstname']
        new_lastname = request.form['lastname']
        sql = " INSERT INTO user (personal_id, firstname, lastname) VALUES (?, ?, ?)"
        cursor = cursor.execute(sql, (new_pers_id, new_firstname, new_lastname))
        conn.commit()
        return f"User with id: {cursor.lastrowid} created succesfully"


@app.route('/monument', methods=['GET', 'POST'])
def monument():
    conn = db_connection()
    cursor = conn.cursor()
    monuments = None
    if request.method == 'GET':
        cursor = conn.execute("SELECT * FROM monument")
        monuments = [dict(id=row[0], name=row[1], description=row[2], category=row[3], image=[4]) for row in
                     cursor.fetchall()]
        if monuments is not None:
            return jsonify(monuments)
        else:
            return "NO monument in db", 404

    if request.method == 'POST':
        new_name = request.form['name']
        new_description = request.form['description']
        new_category = request.form['category']
        new_image = request.form['image']
        sql = " INSERT INTO monument (name, description, category, image) VALUES (?, ?, ?, ?)"
        cursor = cursor.execute(sql, (new_name, new_description, new_category, new_image))
        conn.commit()
        return f"monument with id: {cursor.lastrowid} created succesfully"


@app.route('/insert_mon_csv', methods=['POST'])
def insert_mon_csv():
    csv_data = request.files['csv_monuments']
    monument_df = pd.read_csv(csv_data)

    conn = db_connection()
    cursor = conn.cursor()
    for index, row in monument_df.iterrows():
        monument_data = (row['name'], row['description'], row['category'], row['image'])
        cursor.execute(
            'INSERT INTO monument (name, description, category, image) VALUES (?, ?, ?, ?)', monument_data)

    conn.commit()

    conn.close()

    return 'monumnets inserted correctly in the db'


@app.route('/interaction', methods=['GET', 'POST'])
def interaction():
    conn = db_connection()
    cursor = conn.cursor()

    if request.method == 'GET':
        cursor = conn.execute("SELECT * FROM interaction")
        interaction = [dict(user_id=row[0], monument_id=row[1]) for row in cursor.fetchall()]
        if interaction is not None:
            return jsonify(interaction)

    if request.method == 'POST':
        new_user = request.form['user_id']
        new_monument = request.form['monument_id']
        sql = " INSERT INTO interaction (user_id, monument_id) VALUES (?, ?)"
        cursor = cursor.execute(sql, (new_user, new_monument))
        conn.commit()
        return f"interaction with id: {cursor.lastrowid} created succesfully"


@app.route("/user/<int:id>", methods=['GET', 'DELETE'])
def single_user(id):
    conn = db_connection()
    cursor = conn.cursor()
    if request.method == 'GET':
        cursor.execute("SELECT * FROM user WHERE personal_id = ?", (id,))
        user = cursor.fetchone()
        if user is not None:
            return jsonify(user), 200
        else:
            return 'User not found', 404

    if request.method == 'DELETE':
        sql = "DELETE FROM user WHERE personal_id=?"
        conn.execute(sql, (id,))
        conn.commit()
        return "The user with id {} has been deleted.".format(id), 200


@app.route("/monument/<int:id>", methods=['GET', 'PUT', 'DELETE'])
def single_monument(id):
    conn = db_connection()
    cursor = conn.cursor()
    if request.method == 'GET':
        cursor.execute("SELECT * FROM monument WHERE id = ?", (id,))
        monument = cursor.fetchone()
        if monument is not None:
            return jsonify(monument), 200
        else:
            return 'User not found', 404

    if request.method == 'PUT':
        sql = "UPDATE monument SET name = ?, description = ?, category = ? WHERE id = ?"
        new_name = request.form['name']
        new_description = request.form['description']
        new_category = request.form['category']

        updated_monument = {"id": id, "name": new_name, "description": new_description, "category": new_category}
        conn.execute(sql, (new_name, new_description, new_category, id))
        conn.commit()
        return jsonify(updated_monument), 200

    if request.method == 'DELETE':
        sql = "DELETE FROM monument WHERE id=?"
        conn.execute(sql, (id,))
        conn.commit()
        return "The monument with id {} has been deleted.".format(id), 200


@app.route('/getRecommendation/<int:id>', methods=['GET'])
def get_recommendation(id):
    conn = db_connection()
    #cursor = conn.execute("SELECT id FROM user WHERE personal_id = ?", (id,))
    #user_id = cursor.fetchone()
    cursor = conn.execute("SELECT r1, r2, r3 FROM recommendation WHERE user_id = ?", (id,))
    data = cursor.fetchone()

    if data is not None:
        images = []
        for mon in data:
            cursor.execute("SELECT image FROM monument WHERE name = ?", (mon,))
            images.append(cursor.fetchone())
        to_print = {'monuments': data, 'image': images}
        return jsonify(to_print)
    else:
        cursor = conn.execute("SELECT * FROM recommendation")
        interaction = [dict(user_id=row[0], r1=row[1], r2=row[2], r3=row[3]) for row in cursor.fetchall()]
        if interaction is not None:
            return jsonify(interaction)
        return 'Recommendation not found', 404


if __name__ == '__main__':
    print("initializing app...", file=sys.stderr)
    table_creation()
    update_recommendation_db()
    #app.run(port=8888)

import sqlite3
from importlib.resources import Resource
from flask import Flask, request, jsonify
from flask_cors import CORS
import csv

def connect_to_db(): 
    conn = sqlite3.connect("database.db")
    return conn

def create_db_table(): 
    
    conn = connect_to_db()
    conn.execute('''DROP TABLE IF EXISTS Reviews''')
    conn.execute('''
        CREATE TABLE Reviews(
            ID INTEGER PRIMARY KEY AUTOINCREMENT, 
            Country TEXT NOT NULL,
            Brand TEXT NOT NULL,
            Type TEXT NOT NULL,
            Package TEXT NOT NULL,
            Rating REAL NOT NULL
        );
        '''
        )
    
    with open('ramen-ratings.csv','r') as fin: 
        dr = csv.DictReader(fin)
        to_db = []
        count = 1 
        for i in dr: 
            one_entry = (i['Country'], i['Brand'], i['Type'], i['Package'], i['Rating'])
            to_db.append(one_entry) 
            #count += 1 #changing ids to be incremental
    
    cur = conn.cursor()
    cur.executemany('''
        INSERT INTO Reviews (Country, Brand, Type, Package, Rating) 
        VALUES (?, ?, ?, ?, ?);
        ''', (to_db))
    conn.commit()
    conn.close()

def create_review(review): #review does not need id
    headers = ["Country", "Brand", "Type", "Package", "Rating"]
    inserted_review = {}
    for i in headers: 
        if i not in review: 
            return {"status" : "review creation unsuccessful, please check that JSON contains Country(Text), Brand(Text), Type(Text), Package(Text), Rating(Real)"} 
    try: 
        conn = connect_to_db()
        cur = conn.cursor() 
        cur.execute("SELECT COUNT(*) FROM Reviews")
        new_review_id = cur.fetchone()
        cur.execute("INSERT INTO Reviews (Country, Brand, Type, Package, Rating) VALUES (?, ?, ?, ?, ?)", 
                        (review["Country"], review["Brand"], review["Type"], review["Package"], review["Rating"]))
        conn.commit()
        
        inserted_review = get_review_by_id(new_review_id[0]+1)
    except:
        conn().rollback()
        inserted_review = {"status" : "review creation unsuccessful, please check that JSON containes Country(Text), Brand(Text), Type(Text), Package(Text), Rating(Real)"} 

    finally: 
        conn.close()
    return inserted_review

def get_all_reviews(): 
    all_reviews = []
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM Reviews")
    rows = cur.fetchall()

    # convert row objects to dictionary
    for i in rows:
        review = {}
        review["ID"] = i[0]
        review["Country"] = i[1]
        review["Brand"] = i[2]
        review["Type"] = i[3]
        review["Package"] = i[4]
        review["Rating"] = i[5]
        all_reviews.append(review)
    
    conn.close()
    return all_reviews

def get_review_by_id(id):
    try:
        conn = connect_to_db()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM Reviews WHERE ID = ?", (id,))
        row = cur.fetchone()
        review = {}
        # convert row object to dictionary
        review["ID"] = row["ID"]
        review["Country"] = row["Country"]
        review["Brand"] = row["Brand"]
        review["Type"] = row["Type"]
        review["Package"] = row["Package"]
        review["Rating"] = row["Rating"]
    except:
        review = {"status": "unsuccessful, check ID"}

    return review

def update_review(id,review): #the review can be incomplete 
    message = {}
    flag = 0
    headers = ["Country", "Brand", "Type", "Package", "Rating"]
    try:
        conn = connect_to_db()
        cur = conn.cursor()
        for key in review:
            if key not in headers: continue
            cur.execute(f"UPDATE Reviews SET {key} = ? WHERE ID = ?", (review[key], id)) 

        conn.commit()
        updated_review = get_review_by_id(id)

    except:
        flag = 1
        conn.rollback()
        message["status"] = "test" # "update unsuccesful, check id"
    finally:
        conn.close()
    if flag == 1: 
        return message
    else: return updated_review

def delete_review(id): 
    message = {}
    try:
        conn = connect_to_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM Reviews WHERE ID = ?", (id,))
        count = cur.fetchone()
        if count[0] == 0: 
            message["status"] = "Cannot delete review, check ID"
        else: 
            conn.execute("DELETE from Reviews WHERE ID = ?", (id,))
            conn.commit()
            message["status"] = "Review deleted successfully"
    except:
        conn.rollback()
        message["status"] = "Cannot delete review, check ID"
    finally:
        conn.close()

    return message

def filter_by_country(country): 
    by_country_reviews = []
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM Reviews WHERE Country = ?", (country,))
    rows = cur.fetchall()

    # convert row objects to dictionary
    for i in rows:
        review = {}
        review["ID"] = i[0]
        review["Country"] = i[1]
        review["Brand"] = i[2]
        review["Type"] = i[3]
        review["Package"] = i[4]
        review["Rating"] = i[5]
        by_country_reviews.append(review)
    
    conn.close()
    if by_country_reviews == []: return {"status": "no review that matches given country"}
    return by_country_reviews

def search_by_partial_text(text): 
    text_match_reviews = []
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM Reviews WHERE Type LIKE '%{text}%'")
    rows = cur.fetchall()

    # convert row objects to dictionary
    for i in rows:
        review = {}
        review["ID"] = i[0]
        review["Country"] = i[1]
        review["Brand"] = i[2]
        review["Type"] = i[3]
        review["Package"] = i[4]
        review["Rating"] = i[5]
        text_match_reviews.append(review)
    
    conn.close()
    if text_match_reviews == []: return {"status": "no review with type that matches text given"}
    return text_match_reviews

create_db_table()



app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

@app.route('/reviews', methods=['GET'])
def api_get_all_reviews():
    return jsonify(get_all_reviews())

@app.route('/reviews/<id>', methods=['GET'])
def api_get_review(id):
    return jsonify(get_review_by_id(id))

@app.route('/reviews',  methods = ['POST'])
def api_create_review():
    review = request.get_json()
    return jsonify(create_review(review))

@app.route('/reviews/<id>',  methods = ['PUT'])
def api_update_review(id):
    review = request.get_json()
    return jsonify(update_review(id, review))

@app.route('/reviews/<id>',  methods = ['DELETE'])
def api_delete_review(id):
    return jsonify(delete_review(id))

@app.route('/reviews/country/<country>',  methods = ['GET'])
def api_filter_by_country(country):
    return jsonify(filter_by_country(country))

@app.route('/reviews/text/<partialtext>',  methods = ['GET'])
def api_search_by_partial_text(partialtext):
    return jsonify(search_by_partial_text(partialtext))

if __name__ == "__main__":
    create_db_table()
    #app.run(debug=True)
    app.run()
    
        





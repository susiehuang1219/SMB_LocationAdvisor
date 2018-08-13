import os

from flask import Flask, request, render_template, redirect, url_for, jsonify
from flaskext.mysql import MySQL

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
    )
    '''
    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)
    '''
    return app

app = create_app()
mysql = MySQL()
# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = '{user}'
app.config['MYSQL_DATABASE_PASSWORD'] = '{pwd}'
app.config['MYSQL_DATABASE_DB'] = '{db}'
app.config['MYSQL_DATABASE_HOST'] = '{ip}'
mysql.init_app(app)

@app.route('/getLocations', methods=['GET'])
def get_locations():
    conn = mysql.connect()
    cursor = conn.cursor()
    zipcode = request.args.get('zipcode')
    category = request.args.get('category')
    rv = cursor.execute(
        'SELECT name, stars, latitude, longitude'
        ' FROM business_attr_test'
        ' WHERE postal_code = %s AND categories LIKE %s',
        (zipcode,"%"+category+"%")
    )
    map_data = cursor.fetchall()
    results = {}
    results["table_results"] = []
    results["map_results"] = []
    for row in map_data:
        results["map_results"].append({
            "name": row[0],
            "stars": row[1],
            "latitude": row[2],
            "longitude": row[3],
        })

    rv = cursor.execute(
        'SELECT city, state, postal_code, categories, sentiment, affordability, accessibility, highest_pop'
        ' FROM business_rank_test'
        ' WHERE postal_code = %s AND categories LIKE %s',
        (zipcode,"%"+category+"%")
    )
    table_data = cursor.fetchall()
    for row in table_data:
        results["table_results"].append({
            "city": row[0],
            "state": row[1],
            "zipcode": row[2],
            "category": row[3],
            "sentiment": row[4],
            "affordability": row[5],
            "accessibility": row[6],
            "highest_pop": row[7],
        })
    print(results)
    return jsonify(results)

@app.route("/")
def index():
    return render_template('index.html')

if __name__ == "__main__":
    app.run('0.0.0.0')

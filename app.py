import datetime
import mimetypes
import os
import uuid

import numpy
import psycopg
from dotenv import dotenv_values
from flask import Flask, request, send_file
from flask_cors import cross_origin, CORS
from psycopg import ClientCursor
from sklearn.cluster import AffinityPropagation
from geopy.geocoders import Nominatim

config = dotenv_values(".env")

app = Flask(__name__)
CORS(app, support_credentials=True)

geolocator = Nominatim(user_agent="Paul Duke STEM TSA")
geocache = {}


@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


@app.route('/clusters')
def get_clusters():
    with psycopg.connect(f"dbname={config['DATABASE']} "
                         f"user={config['USERNAME']} "
                         f"password={config['PASSWORD']}", cursor_factory=ClientCursor) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT latitude, longitude, "timestamp" FROM reports')
            raw = cur.fetchall()
            points = numpy.array(list(map(lambda x: (x[0], x[1]), raw)))

            clf = AffinityPropagation()

            clusters = {}
            for (point, cluster) in zip(raw, clf.fit_predict(points)):
                if cluster in clusters:
                    clusters[int(cluster)].append(point)
                else:
                    clusters[int(cluster)] = [point]

            averaged = {}
            for (cluster, arr) in clusters.items():
                avg_lat = sum(map(lambda x: x[0], arr)) / len(arr)
                avg_lng = sum(map(lambda x: x[1], arr)) / len(arr)

                latest = max(map(lambda x: x[2], arr))
                center = f"{avg_lat}, {avg_lng}"

                if center in geocache:
                    city_state = geocache[center]
                else:
                    location = geolocator.reverse(f"{avg_lat}, {avg_lng}", exactly_one=True)
                    possible_keys = ['city', 'town']
                    city = ""

                    for i in possible_keys:
                        if i in location.raw['address']:
                            city = location.raw['address'][i]

                    city_state = f"{city}, {location.raw['address']['state']}"
                    geocache[center] = city_state

                averaged[cluster] = {
                    "location": city_state,
                    "center": (avg_lat, avg_lng),
                    "entries": len(arr),
                    "latest": latest
                }

            return averaged


@app.route('/image', methods=["GET"])
@cross_origin(supports_credentials=True)
def get_image():
    path = f"./image/{request.args.get('file_name')}"
    return send_file(path)


@app.route('/reports', methods=["GET"])
@cross_origin(supports_credentials=True)
def get_reports_in_box():
    southwest = request.args.get("sw")
    northeast = request.args.get("ne")

    sw_lat_long = southwest.split(" ")
    ne_lat_long = northeast.split(" ")

    with psycopg.connect(f"dbname={config['DATABASE']} "
                         f"user={config['USERNAME']} "
                         f"password={config['PASSWORD']}", cursor_factory=ClientCursor) as conn:
        with conn.cursor() as cur:

            cur.execute("SELECT * FROM reports "
                        "WHERE (latitude BETWEEN %s AND %s) AND (longitude BETWEEN %s AND %s)",
                        (sw_lat_long[0], ne_lat_long[0], sw_lat_long[1], ne_lat_long[1]))

            val = []

            for row in cur:
                entry = {}
                for i in range(len(row)):
                    match i:
                        case 0:
                            entry['id'] = row[i]
                        case 1:
                            entry['file_name'] = row[i]
                        case 2:
                            entry['latitude'] = row[i]
                        case 3:
                            entry['longitude'] = row[i]
                        case 4:
                            entry['timestamp'] = row[i]
                val.append(entry)

            return val


@app.route('/upload', methods=["POST"])
@cross_origin(supports_credentials=True)
def upload_report():
    image = request.files['image']
    latitude = request.form['latitude']
    longitude = request.form['longitude']

    extension = mimetypes.guess_extension(image.content_type)
    file_name = str(uuid.uuid4()) + extension

    image.save(f"./image/{file_name}")

    timestamp = datetime.datetime.now()

    with psycopg.connect(f"dbname={config['DATABASE']} "
                         f"user={config['USERNAME']} "
                         f"password={config['PASSWORD']}", cursor_factory=ClientCursor) as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            # Execute a command: this creates a new table
            cur.execute("INSERT INTO reports (file_name, latitude, longitude, timestamp) VALUES (%s, %s, %s, %s)",
                        (file_name, latitude, longitude, timestamp))

            conn.commit()

    return ""


if __name__ == '__main__':
    app.run()

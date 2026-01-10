from flask import Flask, jsonify, request
from flask_cors import CORS
import geopandas as gpd
import pandas as pd
import json

# --- Flask Application Setup ---
app = Flask(__name__, static_folder='public', static_url_path='')
CORS(app)

# --- Helper to fetch GeoJSON data from file ---
def fetch_geojson_from_file(file_path, filter_by=None, filter_value=None):
    try:
        gdf = gpd.read_file(file_path)
        if filter_by and filter_value:
            gdf = gdf[gdf[filter_by] == filter_value]
        return json.loads(gdf.to_json())
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return {"type": "FeatureCollection", "features": []}

# --- Routes ---
@app.route('/')
def index():
    return app.send_static_file('index.html')

@app.route('/api/regions')
def get_regions():
    return jsonify(fetch_geojson_from_file('data/cmr_admin1.geojson'))

@app.route('/api/departments')
def get_departments():
    region_pcode = request.args.get('region_pcode')
    return jsonify(fetch_geojson_from_file('data/cmr_admin2.geojson', 'adm1_pcode', region_pcode))

@app.route('/api/communes')
def get_communes():
    department_pcode = request.args.get('department_pcode')
    return jsonify(fetch_geojson_from_file('data/cmr_admin3.geojson', 'adm2_pcode', department_pcode))

@app.route('/api/productions')
def get_productions():
    try:
        df = pd.read_csv('data/productions.csv')
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        print(f"Error reading productions.csv: {e}")
        return jsonify([])

# Main
if __name__ == '__main__':
    app.run(debug=True, port=5000)

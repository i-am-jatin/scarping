from flask import Flask, request, jsonify, send_from_directory, render_template
import os
import subprocess
from werkzeug.utils import secure_filename
from pymongo import MongoClient
from datetime import datetime

# MongoDB setup
mongo_uri = "mongodb+srv://gaurav444:gaurav444@cluster0.iocsbho.mongodb.net/"
client = MongoClient(mongo_uri)
db = client['test']  # Database name
collection = db['DubaiScrapeLogs']    # Collection name


app = Flask(__name__)

# Configuration
UPLOAD_FOLDER = 'config'
OUTPUT_FOLDER = 'output'
ALLOWED_EXTENSIONS = {'csv', 'xlsx'}

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# ---- Endpoint: Home page ----
@app.route('/')
def index():
    return render_template('index.html')

# ---- Endpoint: Run scrapers ----
@app.route('/run-scraper', methods=['POST'])
def run_scraper():
    selected_scripts = request.form.get('script', '')
    script_list = selected_scripts.split(',')

    # Handle file upload
    if 'config' in request.files:
        file = request.files['config']
        if file and allowed_file(file.filename):
            filename = secure_filename("make_model.csv")
            file.save(os.path.join(UPLOAD_FOLDER, filename))

    # Script map
    scripts = {
        'yango': 'yango_script.py',
        'inygo': 'invygo_script.py',
        'dubizzle': 'dubizzle_script.py',
    }
      
    try:
        executed = []
        for scr in script_list:
            scr = scr.strip()
            if scr in scripts:
                subprocess.run(['python', scripts[scr]], check=True)
                executed.append(scr)
        
        if not executed:
            return "❌ No valid script selected.", 400

        # ✅ Store in MongoDB
        collection.insert_one({
            "scripts_run": executed,
            "scrape": True,
            "timestamp": datetime.utcnow()
        })

        return f"✅ Scraping completed for: {', '.join(executed).capitalize()}"
    
    except subprocess.CalledProcessError as e:
        return f"❌ Error running script: {e}", 500


    # try:
    #     executed = []
    #     for scr in script_list:
    #         scr = scr.strip()
    #         if scr in scripts:
    #             subprocess.run(['python', scripts[scr]], check=True)
    #             executed.append(scr)
        
    #     if not executed:
    #         return "❌ No valid script selected.", 400
        
    #     return f"✅ Scraping completed for: {', '.join(executed).capitalize()}"
    # except subprocess.CalledProcessError as e:
    #     return f"❌ Error running script: {e}", 500

     

# ---- Endpoint: List output files ----
@app.route('/list-outputs')
def list_outputs():
    files = os.listdir(OUTPUT_FOLDER)
    return jsonify(files)

# ---- Endpoint: Download specific output file ----
@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(OUTPUT_FOLDER, filename, as_attachment=True)

# ---- Run the app ----
if __name__ == '__main__':
    app.run(debug=True, port=8000)

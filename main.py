from flask import Flask, request, jsonify
import os

app = Flask(__name__)

@app.route('/imonnit-webhook', methods=['POST'])
def webhook():
    # iMonnit sends data as JSON
    data = request.json
    
    if data:
        print(f"Data Received: {data}")
        # Logic to save to a database or process goes here
        return jsonify({"status": "success"}), 200
    else:
        return jsonify({"status": "error", "message": "No data"}), 400

if __name__ == '__main__':
    # Railway provides the PORT via environment variables
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

    
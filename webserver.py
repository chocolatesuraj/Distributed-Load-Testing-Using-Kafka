from flask import Flask, jsonify
import time
import random

app = Flask(__name__)
metrics = {"requests": 0, "responses": 0}

@app.route('/test')
def test_handler():
    simulate_delay = random.uniform(0, 2)  # Simulate quick response times or introduce delays

    time.sleep(simulate_delay)

    metrics["requests"] += 1
    metrics["responses"] += 1

    return 'OK', 200

@app.route('/metrics')
def metrics_handler():
    return jsonify(metrics)

if __name__ == '__main__':
    app.run(port=8080)

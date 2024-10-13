from flask import Flask, request, jsonify
import subprocess
import os
import signal
import sys

app = Flask(__name__)
process = None

@app.route('/start', methods=['POST'])
def start_server():
    global process
    if process is not None:
        return jsonify({"error": "Server is already running"}), 400

    try:
        config = request.get_json(force=True)
        cluster_size = str(config["cluster_size"])
        proto = config["proto"]
        datatype = config["datatype"]
        addr = config.get("addr", None)
        name = config.get("name", None)
        proto_arg = config.get("proto_arg", None)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    try:
        command = ["demon", "--proto", proto, "--datatype", datatype, "--cluster-size", cluster_size]
        if not addr is None:
            command += ["--addr", addr]
        if not name is None:
            command += ["--replica-name", name]
        if not proto_arg is None:
            command += ["--proto-arg", proto_arg]
        process = subprocess.Popen(command)
        return jsonify({"message": "server started", "pid": process.pid}), 200
    except Exception as e:
        print(e, file=sys.stderr)
        return jsonify({"error": str(e)}), 500

@app.route('/stop', methods=['POST'])
def stop_server():
    global process
    if process is None:
        return jsonify({"error": "No server is running"}), 400

    try:
        os.kill(process.pid, signal.SIGTERM)
        process = None
        return jsonify({"message": "Server stopped"}), 200
    except Exception as e:
        print(e, file=sys.stderr)
        return jsonify({"error": str(e)}), 500

@app.route('/run_cmd', methods=['POST'])
def run_cmd():
    try:
        body = request.get_json(force=True)
        cmd = str(body["cmd"]).split(" ")
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    try:
        output = subprocess.run(cmd, capture_output=True, text=True)
        return jsonify({"message": "success", "std_out": output.stdout, "std_err": output.stderr}), 200
    except Exception as e:
        print(e, file=sys.stderr)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("starting to listen on control port 5000")
    app.run(host="0.0.0.0", port=5000)

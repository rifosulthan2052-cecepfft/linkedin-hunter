from flask import Flask, request, jsonify
import linkedin_hunter  # import your existing script

app = Flask(__name__)

@app.route("/")
def home():
    return "✅ LinkedIn Hunter is live! Use POST /run to trigger the pipeline."

@app.route("/run", methods=["POST"])
def run_pipeline():
    try:
        linkedin_hunter.main()
        return jsonify({"status": "success", "message": "Pipeline finished."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

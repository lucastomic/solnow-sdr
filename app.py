#!/usr/bin/env python3
"""
SolNow Prospector — Web interface with real-time SSE streaming.
Run: python app.py
"""

import json
import os
import queue
import threading
import uuid
from datetime import datetime

from flask import Flask, Response, jsonify, render_template, request, send_file

from prospect import export_excel, run_search

app = Flask(__name__)

JOBS = {}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/search", methods=["POST"])
def start_search():
    data = request.get_json(force=True)

    api_key = data.get("api_key", "").strip() or os.environ.get("GOOGLE_PLACES_API_KEY")
    if not api_key:
        return jsonify({"error": "No se proporcionó API Key (ni en la interfaz ni como variable de entorno)"}), 400

    zones = data.get("zones", [])
    if not zones:
        return jsonify({"error": "No zones provided"}), 400

    queries = data.get("queries", [])
    queries = [q.strip() for q in queries if q.strip()]  # clean empties

    job_id = uuid.uuid4().hex[:12]
    q = queue.Queue()
    JOBS[job_id] = {
        "queue": q,
        "status": "running",
        "zones": zones,
        "result": None,
        "filename": None,
    }

    seen = set()

    def on_place(pid, place):
        if pid in seen:
            return
        seen.add(pid)
        q.put({
            "event": "place",
            "data": {
                "name": place.get("displayName", {}).get("text", ""),
                "address": place.get("formattedAddress", ""),
                "phone": place.get("nationalPhoneNumber") or place.get("internationalPhoneNumber", ""),
                "website": place.get("websiteUri", ""),
                "rating": place.get("rating", ""),
                "reviews": place.get("userRatingCount", ""),
                "zona": place.get("_zona", ""),
            },
        })

    def worker():
        try:
            zone_results = run_search(api_key, zones, on_place=on_place, queries=queries or None)
            JOBS[job_id]["result"] = zone_results

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"prospects_{timestamp}_{job_id}.xlsx"
            filepath = os.path.join("output", filename)
            os.makedirs("output", exist_ok=True)
            export_excel(zone_results, filepath)
            JOBS[job_id]["filename"] = filepath

            total = sum(len(v) for v in zone_results.values())
            q.put({"event": "done", "data": {"total": total, "job_id": job_id}})
            JOBS[job_id]["status"] = "done"
        except Exception as e:
            q.put({"event": "error", "data": {"message": str(e)}})
            JOBS[job_id]["status"] = "error"
        finally:
            q.put(None)  # sentinel

    t = threading.Thread(target=worker, daemon=True)
    t.start()

    return jsonify({"job_id": job_id})


@app.route("/api/stream/<job_id>")
def stream(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    def generate():
        q = job["queue"]
        while True:
            try:
                msg = q.get(timeout=60)
            except queue.Empty:
                yield ":\n\n"  # SSE keepalive comment
                continue

            if msg is None:
                break

            event = msg["event"]
            data = json.dumps(msg["data"], ensure_ascii=False)
            yield f"event: {event}\ndata: {data}\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/download/<job_id>")
def download(job_id):
    job = JOBS.get(job_id)
    if not job or not job.get("filename"):
        return jsonify({"error": "File not ready"}), 404

    return send_file(job["filename"], as_attachment=True,
                     download_name=os.path.basename(job["filename"]))


if __name__ == "__main__":
    app.run(debug=True, port=5000, threaded=True)

"""Capture synthetic Alertmanager notifications for the S4 rehearsal."""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Receiver(BaseHTTPRequestHandler):
    receipts = []

    def do_POST(self):
        if self.path != "/notify":
            self.send_error(404)
            return
        payload = json.loads(
            self.rfile.read(int(self.headers["Content-Length"]))
        )
        self.receipts.extend(
            {"name": alert["labels"]["alertname"], "status": alert["status"]}
            for alert in payload["alerts"]
        )
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        if self.path != "/receipts":
            self.send_error(404)
            return
        body = json.dumps(self.receipts).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8080), Receiver).serve_forever()

import base64
import json
import os

from flask import Flask, request

app = Flask(__name__)


@app.route("/listening", methods=["GET"])
def listening_check():
    """
    Simple healthcheck endpoint so you can verify
    that the Cloud Run service is up and running.
    """
    return "Consumer service is listening", 200


@app.route("/", methods=["POST"])
def receive_pubsub_message():
    """
    Main endpoint for Pub/Sub push.
    Pub/Sub will send a POST request with a JSON body that looks like:

    {
      "message": {
        "data": "<base64-encoded payload>",
        "messageId": "1234567890",
        "attributes": { ... }
      },
      "subscription": "projects/.../subscriptions/..."
    }
    """
    envelope = request.get_json(silent=True)

    if not envelope:
        print("No JSON payload received.")
        return "Bad Request: no JSON", 400

    if "message" not in envelope:
        print("Invalid Pub/Sub message format: no 'message' field.")
        return "Bad Request: no message field", 400

    msg = envelope["message"]

    # Decode the base64-encoded data field
    data = msg.get("data")
    if data:
        try:
            decoded_bytes = base64.b64decode(data)
            decoded_str = decoded_bytes.decode("utf-8")
        except Exception as exc:
            print(f"Error decoding message data: {exc}")
            decoded_str = "<unable to decode>"
    else:
        decoded_str = "<no data>"

    message_id = msg.get("messageId", "<no-id>")
    attributes = msg.get("attributes", {})

    print("==== New Pub/Sub message ====")
    print(f"messageId: {message_id}")
    print(f"attributes: {json.dumps(attributes)}")
    print(f"data (raw string): {decoded_str}")
    print("==== End message ====")

    # Pub/Sub expects a 200 OK to consider the message ACKed.
    # If you return non-2xx, Pub/Sub will retry delivery.
    return "OK", 200


if __name__ == "__main__":
    # Cloud Run sets PORT env var; default to 8080 for local testing.
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

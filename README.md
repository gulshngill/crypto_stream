# Crypto Stream

(WIP)
BTC price streaming from Coinbase (websocket) -> Pub/Sub -> Dataflow -> CloudStorage

1. Install and init the Cloud SDK
2. Enable billing for the project (if needed)
3. Enable the following APIs: Compute Engine, Stackdriver, Cloud Storage, Cloud Storage JSON, Pub/Sub, Cloud Scheduler, Resource Manager, and App Engine.
4. Create service account key and download it to your local machine
5. Set the GOOGLE_APPLICATION_CREDENTIALS env var to: export GOOGLE_APPLICATION_CREDENTIALS=path/to/my/credentials.json
6. Activate virtual enviornment venv
7. Run publisher.py: python publisher.py --project PROJECT --topic TOPIC
8. Run dataflow.py: 
  python dataflow.py \
  --project=$PROJECT_NAME \
  --input_topic=projects/$PROJECT_NAME/topics/TOPIC \
  --output_path=gs://$BUCKET_NAME/output \
  --runner=DataflowRunner \
  --window_size=2 \
  --temp_location=gs://$BUCKET_NAME/temp

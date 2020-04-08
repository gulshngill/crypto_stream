# Crypto Streaming Pipeline


Streaming pipeline using Google Cloud services Pub/Sub and Dataflow.

BTCUSD and ETHUSD ticker data is streamed from <a href=https://docs.pro.coinbase.com/#the-ticker-channel>Coinbase Websocket</a> to Dataflow for correlation analysis and results are then published to a new Pub/Sub topic.

1. Install and init the Cloud SDK
2. Enable billing for the project (if needed)
3. Enable the following APIs: Compute Engine, Stackdriver, Cloud Storage, Cloud Storage JSON, Pub/Sub, Resource Manager, and App Engine.
4. Create service account key and download it to your local machine
5. Set the GOOGLE_APPLICATION_CREDENTIALS env var to: export GOOGLE_APPLICATION_CREDENTIALS=path/to/my/credentials.json
6. Install requirements.txt
7. Run publisher.py: python publisher.py --project=PROJECT --topic=TOPIC
8. Run dataflow.py: 
  python dataflow.py \
  --project=$PROJECT_NAME \
  --input_topic=projects/$PROJECT_NAME/topics/topic-name \
  --output_topic=projects/$PROJECT_NAME/topics/topic-name \
  --runner=DataflowRunner \
  --temp_location=gs://$BUCKET_NAME/temp

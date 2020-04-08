'''
python dataflow.py \
  --project=$PROJECT_NAME \
  --input_topic=projects/$PROJECT_NAME/topics/topic-name \
  --output_topic=projects/$PROJECT_NAME/topics/topic-name \
  --runner=DataflowRunner \
  --temp_location=gs://$BUCKET_NAME/temp
'''
import argparse
import json
import logging
import apache_beam as beam
import dateutil.parser as dp
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
from apache_beam.utils.timestamp import Duration

PAIR = "BTC-ETH"
CORRELATION_THRESHOLD = -0.70


class JsonCoder(object):
    """A JSON coder interpreting each line as a JSON string."""

    def encode(self, x):
        return json.dumps(x).encode('utf-8')

    def decode(self, x):
        return json.loads(x)


class ParseData(beam.DoFn):
    """Parse and reshape data to only include required fields and filter out header response"""

    def process(self, elem):
        try:

            row = json.loads(elem)

            if 'channels' not in row.keys():
                # TODO: Consider reducing precision of timestamp to
                yield 'BTC-ETH', (row['time'], row['product_id'], row['price'])

        except Exception as e:
            # TODO: Collect erroneous records, output to separate location
            logging.error(f"ERROR: {repr(e)}\nSTEP: {self.__class__.__name__}\nELEMENT: {elem}")


class AddTimeStampFn(beam.DoFn):
    """Assign timestamp to element for windowing"""

    def process(self, elem):
        try:
            _, data = elem
            unix_timestamp = dp.parse(data[0]).timestamp()

            yield beam.window.TimestampedValue(elem, unix_timestamp)

        except Exception as e:
            # TODO: Collect erroneous records, output to separate location
            logging.error(f"ERROR: {repr(e)}\nELEMENT: {elem}")


class CalculateCorrelation(beam.DoFn):
    """Calcualte correlation of price"""

    def process(self, elem):
        _, data = elem

        btc_price_data = [price for price in data if price[1] == 'BTC-USD']
        eth_price_data = [price for price in data if price[1] == 'ETH-USD']

        if btc_price_data and eth_price_data:
            index, _, price = zip(*btc_price_data)  # unzip

            print(f"BTC {index}, {price}")

            btc_series = pd.Series([float(p) for p in price],
                                   index=[pd.Timestamp(i) for i in index])

            index, _, price = zip(*eth_price_data)  # unzip

            print(f"ETC {index}, {price}")

            eth_series = pd.Series([float(p) for p in price],
                                   index=[pd.Timestamp(i) for i in index])

            print(btc_series.corr(eth_series, method='pearson'))

            yield json.dumps((PAIR, btc_series.corr(eth_series, method='pearson'))).encode('utf-8')

        else:
            yield PAIR, 0


def run(input_topic, output_topic, pipeline_args=None):
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True)

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'Read PubSub Messages' >> beam.io.ReadFromPubSub(topic=input_topic)
         # | 'ReadText' >> beam.io.ReadFromText("data.json")
         | 'ParseJSON' >> beam.ParDo(ParseData())
         | 'AddEventTs' >> beam.ParDo(AddTimeStampFn())
         | 'Windowing' >> beam.WindowInto(
                    window.SlidingWindows(size=120, period=60),
                    allowed_lateness=Duration(seconds=60))
         | 'GroupByKey' >> beam.GroupByKey()
         | 'CalculateCorrelation' >> beam.ParDo(CalculateCorrelation())
         | 'FilterCorrelation' >> beam.Filter(lambda x: x[1] < CORRELATION_THRESHOLD)
         | 'PublishCorrelation' >> beam.io.WriteToPubSub(output_topic))
        # | 'WriteText' >> beam.io.WriteToText("gs://bucket/output.csv"))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_topic',
        help='The Cloud Pub/Sub topic to read from.\n'
             '"projects/<PROJECT_NAME>/topics/<TOPIC_NAME>".')
    parser.add_argument(
        '--output_topic',
        help='Output topic for correlation')

    known_args, pipeline_args = parser.parse_known_args()

    run(known_args.input_topic, known_args.output_topic, pipeline_args)

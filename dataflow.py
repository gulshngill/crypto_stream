'''
python dataflow.py \
  --project=$PROJECT_NAME \
  --input_topic=projects/$PROJECT_NAME/topics/cron-topic \
  --output_path=gs://$BUCKET_NAME/samples/output \
  --runner=DataflowRunner \
  --window_size=2 \
  --temp_location=gs://$BUCKET_NAME/temp
'''
import argparse
import json
import logging
import time
import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import trigger



class JsonCoder(object):
    """A JSON coder interpreting each line as a JSON string."""

    def encode(self, x):
        return json.dumps(x).encode('utf-8')

    def decode(self, x):
        return json.loads(x)



class ParseData(beam.DoFn):
    """Parse data to only include required fields and filter out header record"""
    def __init__(self):
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        try:

            if 'channels' not in elem.keys():

                elem["time"] = elem["time"].replace('T', ' ').replace('Z','')

                yield {
                'product_id': elem["product_id"],
                'trade_id': elem["trade_id"],
                'sequence': elem["sequence"],
                'timestamp': elem["time"],
                'price': elem["price"],
                'best_bid': elem["best_bid"],
                'best_ask': elem["best_ask"]
                }

                logging.info('Parsed: %s', elem)

        except:
          # Log and count parse errors
          self.num_parse_errors.inc()
          logging.error('Parse error on "%s"', elem)


class CalculateSpread(beam.DoFn):
    """Calculate spread amount and %"""

    def __init__(self):
        beam.DoFn.__init__(self)
        self.spread_errors = Metrics.counter(self.__class__, 'spread_errors')

    def process(self, elem):
        try:
            spread_amt = float(elem["best_ask"]) - float(elem["best_bid"])
            spread_percentage = spread_amt / float(elem["best_ask"]) * 100

            yield {
            'product_id': elem["product_id"],
            'trade_id': elem["trade_id"],
            'sequence': elem["sequence"],
            'timestamp': elem["timestamp"],
            'price': elem["price"],
            'best_bid': elem["best_bid"],
            'best_ask': elem["best_ask"],
            'spread_amt': "{:.2f}".format(spread_amt),
            'spread_percentage': "{:.2f}".format(spread_percentage)
            }

            logging.info('Parsed: %s', elem)

        except:
          # Log and count parse errors
          self.spread_errors.inc()
          logging.error('error on "%s" when calculating spread', elem)




def run(input_topic, output_path, window_size=1.0, pipeline_args=None):
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True)

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         #| 'Read PubSub Messages' >> beam.io.ReadFromPubSub(topic=input_topic)
         | 'ReadText' >> beam.io.ReadFromText("data.json", coder=JsonCoder())
         | 'ParseJSON' >> beam.ParDo(ParseData())
         | 'CalculateSpread' >> beam.ParDo(CalculateSpread())
         | 'AddEventTs' >> beam.Map(lambda elem: beam.window.TimestampedValue(elem, time.mktime(
                    time.strptime(elem['timestamp'], '%Y-%m-%d %H:%M:%S.%f'))))
         # | 'GlobalWindow' >> beam.WindowInto(
         #    beam.window.GlobalWindows(),
         #    trigger=trigger.Repeatedly(trigger.AfterProcessingTime(1 * 60)), #maybe use afterwatermark(early,late) after watermark only triggers when windowcloses
         #    accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
         | 'WriteText' >> beam.io.WriteToText("output.csv"))
         #| 'Write to GCS' >> beam.ParDo(WriteBatchesToGCS(output_path)))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_topic',
        help='The Cloud Pub/Sub topic to read from.\n'
             '"projects/<PROJECT_NAME>/topics/<TOPIC_NAME>".')
    parser.add_argument(
        '--window_size',
        type=float,
        default=1.0,
        help='Output file\'s window size in number of minutes.')
    parser.add_argument(
        '--output_path',
        help='GCS Path of the output file including filename prefix.')
    known_args, pipeline_args = parser.parse_known_args()

    run(known_args.input_topic, known_args.output_path, known_args.window_size,pipeline_args)
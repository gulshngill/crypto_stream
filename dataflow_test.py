"""Test Dataflow pipeline"""

import logging
import unittest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

import apache_beam as beam
import dataflow


class ParseDataTest(unittest.TestCase):
    """Test parsing of input data"""

    def setUp(self):
        logging.getLogger().setLevel(logging.DEBUG)

    test_parse_expected_results = [('BTC-ETH', ('2020-04-28T11:16:36.458552Z', 'BTC-USD', '6285')),
                                   ('BTC-ETH', ('2020-04-28T11:16:48.693187Z', 'BTC-USD', '6394')),
                                   ('BTC-ETH', ('2020-04-28T11:16:58.693187Z', 'BTC-USD', '6500')),
                                   ('BTC-ETH', ('2020-04-28T11:16:36.458552Z', 'ETH-USD', '170')),
                                   ('BTC-ETH', ('2020-04-28T11:16:48.693187Z', 'ETH-USD', '150')),
                                   ('BTC-ETH', ('2020-04-28T11:16:58.693187Z', 'ETH-USD', '120'))
                                   ]

    test_correlation_expected_results = ['["BTC-ETH", -0.9924429712315698]'.encode('utf-8')]

    def test_parse_data(self):
        """Tests parsing of input data"""
        with TestPipeline() as p:
            result = (p | 'ReadText' >> beam.io.ReadFromText("data.json")
                      | 'ParseJSON' >> beam.ParDo(dataflow.ParseData()))

            assert_that(
                result, equal_to(self.test_parse_expected_results))

    def test_correlation(self):
        """Tests correlation result of input data"""
        with TestPipeline() as p:
            result = (p | 'ReadText' >> beam.io.ReadFromText("data.json", coder=dataflow.JsonCoder())
                      | 'FilterHeader' >> beam.Filter(lambda elem: 'channels' not in elem.keys())
                      | 'ParseData' >> beam.Map(
                        lambda elem: ('BTC-ETH', (elem['time'], elem['product_id'], elem['price'])))
                      | 'GroupByKey' >> beam.GroupByKey()
                      | 'CalculateCorrelation' >> beam.ParDo(dataflow.CalculateCorrelation())
                      )
            assert_that(
                result, equal_to(self.test_correlation_expected_results))


if __name__ == '__main__':
    unittest.main()

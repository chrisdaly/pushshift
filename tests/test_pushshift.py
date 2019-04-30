import unittest
import requests
import json
import datetime
from pushshift import PushshiftClient


class TestAPI(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        file_path = "./tests/spinalmuscularatrophy.json"
        with open(file_path, "r", encoding='utf-8') as f:
            self.local_data = json.loads(f.read())

        start = datetime.datetime(2018, 1, 11)
        end = datetime.datetime(2018, 1, 13)
        subreddit = "spinalmuscularatrophy"

        params = {
            'after': str(PushshiftClient.datetime_to_epoch(start)),
            'before': str(PushshiftClient.datetime_to_epoch(end)),
            'subreddit': subreddit
        }

        pushshift_api = PushshiftClient()
        self.api_data = pushshift_api.get_all_content(include_context=True, **params)

    def test_structure(self):
        self.assertIsInstance(self.api_data, dict)

    def test_length(self):
        self.assertEquals(len(self.api_data), len(self.local_data))


if __name__ == '__main__':
    loader = unittest.TestLoader()
    test_classes_to_run = [TestAPI]

    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)

    big_suite = unittest.TestSuite(suites_list)
    runner = unittest.TextTestRunner()
    results = runner.run(big_suite)

'''
Created on 12. 11. 2018

@author: esner
'''
import unittest
import mock
import os
from freezegun import freeze_time
from io import StringIO
import src.component as cp

from src.component import Component


class TestComponent(unittest.TestCase):

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    @freeze_time("2010-10-10")
    def test_get_append_timestamp(self):
        timestamp = cp.get_append_timestamp(True)
        self.assertEqual("_20101010000000", timestamp)

    def test_fix_path(self):
        self.assertEqual(cp.fix_path("/test"), "/test/")
        self.assertEqual(cp.fix_path("/test/"), "/test/")

    def test_parse_private_key_throws_error_on_invalid_key(self):
        with self.assertRaises(IndexError):
            cp._parse_private_key(StringIO("key"))

    def test_parse_private_key_throws_error_on_invalid_key(self):
        key = cp._parse_private_key(StringIO(
            "-----BEGIN RSA PRIVATE KEY-----\nMIIEogIBAAKCAQEAsH4Y5UUUCHiD7OkNEjHhZeqOnbIv2/Sr3jzz+DrkGvAlEGwT"
            "\n7btrqWuqZT/cX3x1B0wiMqu3zMC+78Gy5bdNau7BJpN5FjwAzzDKVArR47ZIlyKO\nKGhRvafq2pZGQh9YUYsECzA2yoJdJTMfc"
            "/D1x1K6BGSXd7hnFDNtyMiXu9/7KRQ8\nHNZ8R78BNp7lrzV0fLMC/61n5mmXxXTVS2z6JCr8fSxNaYEEqt2aZra6Rl6c9D7O"
            "\njA15PcvXqojqSNhsrN9bslPX+F/16aUzqtCDwJcsEIrY8e6SpvDhbJeXr+wDccqf"
            "\nz4HFmPqFCNU4jm7qQdDfuFOW9BCVSTcX74vOcwIDAQABAoIBAETmpW90HVMFQXOO\ns"
            "+SjhnwUKuMTei2jgik7oH8K9pwxnjagCtOndGtqtdXbLXw1iTZ1GXCwqwuLP783\n3lBh1B5n4Q3fSslMWYCJaqOOqcv9EK+39Ml"
            "/mFGzKTN2sS0FMaR74fNAOlOquxRX\ntfK8YicTe71VS/CYE93GChj1fo8ARpKfgaUrtJ3bqNcJjuiNwcSaOug3BE+Vtode"
            "\nqsxVEcMxM9tZd61iqJ6kSrOLUZKaXGimTRS5zCb9cyjhTm7YBQtFFATS/NHNk7lq"
            "\n1vfNVWMgVa7afLWgbd7C6xWUZ1wbpWK5F8fdYeUrwZ37fja9Pl1AnuVS7UvdVJvZ"
            "\ntp7UXDECgYEA5LnU4Ynw0QDH7oNhcw2WGs9NzRja6aU2QPLCfBrNbZ8e8pU34Koz"
            "\n7f3B5uyNA7uf1G9dNNJNsT2e33OKURggojz03qlD5HdetqxDzJ6LweE3LFikwGgy\nV5z2AK6CsMIl46YoEz1wI"
            "+sO1QmcrjKp2dcxxaeexGuYe3xAZ77kHrkCgYEAxYnF\nsJjyeosDIhULGUxBe2nbFaNnS4yI0W8dyif4MJz9zMPFNPS"
            "/xAZXZIM30fV1XDIL\nrYbj+9K7ptV2dJt8aDd7T3WtBtt74jSPRorep9Ur+va0M3Phjrnimu/GoHZfuCGK"
            "\nXrGyHjTxeRZfkkVSciinKkFbSzIkbVFZFMiYIIsCgYB4hWckFNBdAQFYr8fYnS8c\nH2IKkW9AsDp"
            "/TKuoQ2M9wRvIjVItQuIsJItYyAqiDepxQOEnJS2lGCgv7CzVAFap\nxl7tONm6eB"
            "/jN7BeEKjp12eAKZFehUkJm36Q62OYCiV26CWzxarickiVfwQdUjrn"
            "\nu5nRYbqqG1v0rYsuX4rKmQKBgFDJAVIxmqjHBScBGCLmbrk8F18IDox1Etcj7Djq\nk7O94IXHYnU"
            "/ytUuCruOdlulWLO1u9Thn4czLY8TKXiSxhQQ7JsYcwSk6kseV6Hv\n1RMqOOxPzG5ma85k8umOOdsRzh+Nh"
            "/smDMQRvtdYcQlu1ELfoU3EoMNl5EPYyueX\nCa/1AoGAZpHrBNdvroylQnwx7zKfr6SjZXF5ILRc6HfaZqGymGOTdoYIKSC3wQhW"
            "\nPwSfz6myqmw5xduj1QGNPrFFX5xjsTk6YKvbsFP75YnWEWrCCvFS3CFh337VqKSz\no/Jn20IHb/dgZLP5Ff+QeqtbN"
            "/0hBvJeqp7LX3Rdd0EOq1q9OpE=\n-----END RSA PRIVATE KEY-----"))
        self.assertEqual(key.size, 2048)

    def test_get_private_key_with_none(self):
        key = cp.get_private_key("")
        self.assertEqual(key, None)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

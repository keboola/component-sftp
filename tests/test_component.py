'''
Created on 12. 11. 2018

@author: esner
'''
import os
import unittest
from os import path
from os.path import dirname

import mock
import paramiko
from freezegun import freeze_time

from src.component import Component, UserException

TEST_DIR = path.join(dirname(path.realpath(__file__)), 'test_data')
TEST_DIR_TIMESTAMP = path.join(dirname(path.realpath(__file__)), 'test_data_timestamp')
TEST_DIR_KEYONLY = path.join(dirname(path.realpath(__file__)), 'test_data_keyonly')

# Throwaway keys generated only for these tests (never used against a real host).
# They exercise the non-RSA fallback parsers, which only work because
# _parse_private_key rewinds the buffer (keyfile.seek(0)) between attempts.
ECDSA_PRIVATE_KEY = """-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAaAAAABNlY2RzYS
1zaGEyLW5pc3RwMjU2AAAACG5pc3RwMjU2AAAAQQR2XDVndS3a75jo5q1gLgiGgEaNFjzv
xBRefrMMmi1QGj6X3H0sPJ8BDdbm+bL+GaKj0fTBloC5SuMMqRW85ZDmAAAAsDeYOvI3mD
ryAAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBHZcNWd1LdrvmOjm
rWAuCIaARo0WPO/EFF5+swyaLVAaPpfcfSw8nwEN1ub5sv4ZoqPR9MGWgLlK4wypFbzlkO
YAAAAgR9ooXptoDu1/tEVFP7id8kLguMCyewyqgOUNE2RtMZsAAAASdGhyb3dhd2F5LXRl
c3Qta2V5AQIDBAUG
-----END OPENSSH PRIVATE KEY-----"""

ED25519_PRIVATE_KEY = """-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACAQtEzIY06QrD9dJunJ0qMhs1slG4pTMK7ilsoF3OYzmwAAAJjmoJLD5qCS
wwAAAAtzc2gtZWQyNTUxOQAAACAQtEzIY06QrD9dJunJ0qMhs1slG4pTMK7ilsoF3OYzmw
AAAEAHiBSJY4Gx4AxT3Z/T2QChbEAvBNLBBytxvJyBnBlCRxC0TMhjTpCsP10m6cnSoyGz
WyUbilMwruKWygXc5jObAAAAEnRocm93YXdheS10ZXN0LWtleQECAw==
-----END OPENSSH PRIVATE KEY-----"""


class TestComponent(unittest.TestCase):
    @mock.patch.dict(os.environ, {'KBC_DATADIR': TEST_DIR})
    def setUp(self):
        self.comp = Component()

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    @freeze_time("2010-10-10")
    def test_get_output_destination(self):
        input_table = self.comp.get_input_tables_definitions()[0]
        output_destination = self.comp.get_output_destination(input_table)
        self.assertEqual(output_destination, "/path/test_20101010000000.csv")

    @freeze_time("2010-10-10")
    @mock.patch.dict(os.environ, {'KBC_DATADIR': TEST_DIR_TIMESTAMP})
    def test_get_output_destination_custom(self):
        comp = Component()
        input_table = comp.get_input_tables_definitions()[0]
        output_destination = comp.get_output_destination(input_table)
        self.assertEqual(output_destination, "/path/test_2010-10-10-00:00:00.csv")

    def test_parse_private_key_throws_error_on_invalid_key(self):
        with self.assertRaises(UserException):
            self.comp.get_private_key({"#private_key": "key"})

    def test_parse_private_key_rsa(self):
        key = self.comp.get_private_key({"#private_key":
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
            "/0hBvJeqp7LX3Rdd0EOq1q9OpE=\n-----END RSA PRIVATE KEY-----"})
        self.assertEqual(key.size, 2048)

    def test_get_private_key_with_none(self):
        key = self.comp.get_private_key({})
        self.assertEqual(key, None)

    def test_parse_private_key_non_rsa_fallback(self):
        # The RSA parser consumes the buffer; without the seek(0) rewind between
        # attempts, these non-RSA keys would fail to parse. Guards that fix.
        cases = [
            (ECDSA_PRIVATE_KEY, paramiko.ECDSAKey),
            (ED25519_PRIVATE_KEY, paramiko.Ed25519Key),
        ]
        for keystring, expected_type in cases:
            with self.subTest(key_type=expected_type.__name__):
                key = self.comp.get_private_key({"#private_key": keystring})
                self.assertIsInstance(key, expected_type)

    @mock.patch.dict(os.environ, {'KBC_DATADIR': TEST_DIR_KEYONLY})
    @mock.patch.object(Component, 'connect_to_server')
    def test_key_only_config_uses_none_password(self, mock_connect):
        # A key-only config has no '#pass'; run()/test_connection() must not raise
        # KeyError and must call connect_to_server with password=None (bug #2).
        comp = Component()
        comp.test_connection()
        mock_connect.assert_called_once()
        args = mock_connect.call_args.args
        # signature: (port, host, user, password, pkey, disabled_algorithms, banner_timeout)
        self.assertIsNone(args[3])
        self.assertIsInstance(args[4], paramiko.ECDSAKey)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

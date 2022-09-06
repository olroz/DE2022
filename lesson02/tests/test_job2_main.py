"""
Tests for job1 main.py
"""
import sys
from unittest import TestCase, mock
sys.path.append("/Users/olegr/work/_mine/DataEngineering/py/DE2022_my/lesson02/job2")
from job2 import main


class Job2MainFunctionTestCase(TestCase):
    """
    Main tests for job2
    """
    print("******* job2 test main:")

    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()


    def test_return_400_params_missed(self):
        """
        Raise 400 HTTP code when no 'raw_dir' and/or 'stg_dir' param
        """
        resp = self.client.post(
            '/',
            json={
                'raw_dir': '/foo/bar/',
                # no 'stg_dir' set
            },
        )
        resp2 = self.client.post(
            '/',
            json={
                'stg_dir': '/foo/bar/',
                # no 'raw_dir' set
            },
        )
        self.assertEqual(400, resp.status_code)
        self.assertEqual(400, resp2.status_code)


    @mock.patch('main.converter.avro_to_disk')
    def test_api_get_sales_called(
            self,
            avro_to_disk: mock.MagicMock,
    ):
        """
        Test whether converter.avro_to_disk is called with proper params
        """
        fake_raw_dir = '/faa/bar'
        fake_stg_dir = '/foo/bar'
        self.client.post(
            '/',
            json={
                'raw_dir': fake_raw_dir,
                'stg_dir': fake_stg_dir,
            },
        )

        avro_to_disk.assert_called_with(fake_raw_dir, fake_stg_dir)

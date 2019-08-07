import os
import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class ConnectorsTestCase(DataprocTestCase):
    COMPONENT = 'connectors'
    INIT_ACTIONS = ['connectors/connectors.sh']
    FIRST_TEST_DATA_FILENAME = 'first.txt'
    SECOND_TEST_DATA_FILENAME = 'second.txt'
    FIRST_TEST_DATA_PATH = 'test_data/append/{}'.format(FIRST_TEST_DATA_FILENAME)
    SECOND_TEST_DATA_PATH = 'test_data/append/{}'.format(SECOND_TEST_DATA_FILENAME)

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         self.SECOND_TEST_DATA_PATH), name)
        self.__run_test(name)
        self.remove_test_script(self.SECOND_TEST_DATA_FILENAME, name)

    def __run_test(self, name):
        second_test_data_gs_path = '{}/{}/{}'.format(self.INIT_ACTIONS_REPO, self.COMPONENT, self.SECOND_TEST_DATA_PATH)
        self.assert_instance_command(
            name,
            "hadoop fs -appendToFile {} {}".format(self.FIRST_TEST_DATA_FILENAME, second_test_data_gs_path)
        )
        _, stdout, _ = self.assert_instance_command(
            name, "hadoop fs -cat {}".format(second_test_data_gs_path))
        self.assertEqual(stdout, 'Hello world')

    @parameterized.expand(
        [
            ("STANDARD", "1.3", ["m"], "1.9.14"),
            ("STANDARD", "1.4", ["m"], "1.9.14"),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_append(self, configuration, dataproc_version, machine_suffixes, gcs_connector_version):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version=dataproc_version,
                           gcs_connector_version=gcs_connector_version)
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    unittest.main()

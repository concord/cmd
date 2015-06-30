import unittest
import json
import tempfile
import shutil
import re
from concord_cli.deploy import *
from testing_utilities import *
from subprocess import call

class StubParser:
    def error(self, error_string):
        raise RuntimeError(error_string)


class TestDeployScript(unittest.TestCase):

    def setUp(self):
        self.requiredKeys = ['compress_files', 'executable_name', 'computation_name',
                             'zookeeper_hosts', 'zookeeper_path']
        self.validKeys = ["executable_arguments", "docker_container",
                          "fetch_url", "mem", "disk", "cpus",
                          "framework_v_module", "instances",
                          "framework_logging_level", "environment_variables",
                          "zookeeper_hosts", "exclude_compress_files",
                          "update_binary", "execute_as_user",
                          "docker_container"]
        self.filename = 'test_deploy_file'
        self.improperFormatFilename = 'test_deploy_file_incorrect'
        self.stub = StubParser()

    def test_validate_json_raw_config(self):
        valid_dict = dict.fromkeys(self.validKeys)
        req_dict = dict.fromkeys(self.requiredKeys)

        # Test that an empty config file fails
        self.assertRaises(RuntimeError, validate_json_raw_config, {}, self.stub)

        # Even if all optional keys exist, method should fail
        self.assertRaises(RuntimeError,
                          validate_json_raw_config,
                          valid_dict,
                          self.stub)

        # Even a dictionary with at least one invalid key fails
        test_config_dict = req_dict.copy()
        test_config_dict['Invalid Key'] = 'Invalid Value'
        self.assertRaises(RuntimeError,
                          validate_json_raw_config,
                          test_config_dict,
                          self.stub)

        # A dictionary must have all of the required keys
        test_config_dict = req_dict.copy()
        test_config_dict.pop("compress_files")
        self.assertRaises(RuntimeError,
                          validate_json_raw_config,
                          test_config_dict,
                          self.stub)

        # A dictionary with only the required keys passes
        validate_json_raw_config(req_dict, self.stub)


    def test_parse_file(self):
        # Assert that parser fails if file isn't in JSON format
        self.assertRaises(ValueError,
                          parseFile,
                          test_filepath(self.improperFormatFilename),
                          self.stub)

        # Read test configuration file
        data = {}
        abs_path = test_filepath(self.filename)
        with open(abs_path) as data_file:
            data = json.load(data_file)

        cData = DEFAULTS.copy()
        cData.update(data)

        # Retrieve configuration options from method
        conf = parseFile(abs_path, self.stub)

        # Assert that the executable name exists as one of the compress_files
        self.assertTrue(conf["executable_name"] in conf["compress_files"])

        # Assert that the options dictionary contains expected key/value pairs
        self.assertDictEqual(cData, conf)

    def test_tar_file_list(self):
        total_files = [ 'test_directory/a1', 'test_directory/a2',
                        'test_directory/a3',  'test_directory/a4',
                        'test_directory/a5',  'test_directory/a6',
                        'test_directory/b1', 'test_directory/b2',
                        'test_directory/subdir/a1', 'test_directory/subdir/b1']

        # Assert that nothing gets inserted (using empty whitelist)
        white_list = []
        black_list = [ '.*a6' ]
        succ_list = []
        self.tar_tester_driver(total_files, white_list, black_list, succ_list)

        # Assert that nothing gets inserted (using blacklist with greedy glob)
        white_list = list(total_files)
        black_list = [ '.*' ]
        succ_list = []
        self.tar_tester_driver(total_files, white_list, black_list, succ_list)

        # Assert that pattern matching is working to remove correct file kinds
        # and also verify that matching works even in subdirectories
        white_list = list(total_files)
        black_list = [ '.*a' ]
        succ_list = [ 'test_directory/b1', 'test_directory/b2',
                      'test_directory/subdir/b1']
        self.tar_tester_driver(total_files, white_list, black_list, succ_list)


    def tar_tester_driver(self, total_files, white_list, black_list, successful):
        # Create actual temporary directory
        temp_dirname = tempfile.mkdtemp();

        # Pre-pend all strings in both lists with 'temp_dirname/'
        def prepend_dirname(item):
            return temp_dirname + '/' + item

        white_list = map(prepend_dirname, white_list)
        total_files = map(prepend_dirname, total_files)
        successful = map(prepend_dirname, successful)

        try:
            # Create new files from fake data
            for f in total_files:
                create_temporary_file(f)

            # Assert that only elements on the white list remain
            all_include = tar_file_list(white_list, black_list)
            all_include.sort()
            successful.sort()
            self.assertListEqual(all_include, successful)
        finally:
            # Remove temporary directory and all of its related content
            shutil.rmtree(temp_dirname)

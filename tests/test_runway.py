import unittest
from random import randint
from concord.runway import *
from testing_utilities import *

TEST_ROOT = os.path.dirname(os.path.realpath(__file__))
OK_JSON = "{}/runway/runway_good.json".format(TEST_ROOT)
MISSING_DEFAULT = "{}/runway/missing_default.json".format(TEST_ROOT)
MISSING_TYPE = "{}/runway/missing_type.json".format(TEST_ROOT)

class MissingTypeException(CmdTestException):
    pass

class TestRunway(unittest.TestCase):
    def setUp(self):
        self.good_prompt_args = json_from_file(OK_JSON)['prompt_args']
        self.missing_default = json_from_file(MISSING_DEFAULT)['prompt_args']
        self.missing_type = json_from_file(MISSING_TYPE)['prompt_args']
        
    def test_collect_args(self):
        def stub_fn(obj):
            if 'type' not in obj:
                raise MissingTypeException("Missing type arg")
            if obj['type'] == "number":
                return randint(0, 100)
            elif obj['type'] == "boolean":
                return bool(randint(0, 1))
            else:
                return str(randint(0, 100000)) # Doesn't matter            

        def perform_test(stub_data, assertion):
            for obj in stub_data:                
                if assertion is None:
                    # If any exception is raised then test fails
                    collect_argument(obj, stub_fn)
                else:
                    # Expect an exception to raise unless test fails
                    with self.assertRaises(assertion):
                        collect_argument(obj, stub_fn)

        perform_test(self.good_prompt_args, None)
        perform_test(self.missing_type, MissingTypeException)

    def test_collect_enter(self):
        """Tests to see if defaualt is taken when enter is used"""
        # TODO: Not super important but implement this case
        # for obj in self.good_prompt_args:
        #     collect_argument(obj, test_fn)
        
        # Test that Exception is thrown if required is False and default is None
        with self.assertRaises(Exception):
            for obj in self.missing_default:
                collect_argument(obj, lambda x: "")

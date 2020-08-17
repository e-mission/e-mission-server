import logging
import argparse
import importlib
import traceback

import unittest.loader as loader
import unittest.runner as runner

import emission.tests.common as etc

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("branch", help="e.g. master, gis-based-mode-detection")
    parser.add_argument("test_module",
        help="""module containing the test
            e.g. emission.tests.analysisTests.intakeTests.TestPipelineRealData""")
    parser.add_argument("test_class",
        help="""the class (subclass of unittest.TestCase) in the module
            e.g. TestPipelineRealData""")
    parser.add_argument("--test",
        help="""the test to be run 
            e.g. testJumpSmoothingSectionsStraddle""")
    parser.add_argument("--clean", default=False, action='store_true',
        help="""Clear out the existing data, since we don't automatically do so at the end of a test""")
    parser.add_argument("--verbose",
        help="""whether the result output should be verbose""", default=True)

    args = parser.parse_args()
    print(args)

    # spec = importlib.util.spec_from_file_location("realtests", args.test_file_path)
    # module = importlib.util.module_from_spec(spec)
    module = importlib.import_module(args.test_module)
    testcls = getattr(module, args.test_class)
    suite = loader.TestLoader().loadTestsFromTestCase(testcls)

    for (idx, test) in enumerate(suite):
        logging.info("Checking test %s: %s" % (idx, test._testMethodName))
        if args.test is not None and test._testMethodName == args.test:
            if args.clean:
                logging.info("Cleaning test %s: %s" % (idx, test._testMethodName))
                test.branch = args.branch
                etc.fillExistingUUID(test)
                result = test.tearDown()
            else:
                logging.info("Running test %s: %s" % (idx, test._testMethodName))
                test.evaluation = True
                test.persistence = True
                test.branch = args.branch
                result = test()
                print("====== errors =======")
                for (tc, error) in result.errors:
                    print("------ %s -------" % tc)
                    print(error)
                print("====== failures =======")
                for tc, failure in result.failures:
                    print("------ %s -------" % tc)
                    print(failure)

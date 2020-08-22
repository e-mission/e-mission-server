from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
# Test the base class enhancements over AttrDict
# Since the base class should not really contain any properties, we create a
# dummy subclass here and use it for testing.

# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *
import unittest
import enum

# Our imports
import emission.core.wrapper.wrapperbase as ecwb

class TestEnum(enum.Enum):
    A = 0
    B = 1
    C = 2

class TestWrapper(ecwb.WrapperBase):
    props = {"a": ecwb.WrapperBase.Access.RO,
             "b": ecwb.WrapperBase.Access.RO,
             "c": ecwb.WrapperBase.Access.RO,
             "WrapperBase": ecwb.WrapperBase.Access.RO,
             "invalid": ecwb.WrapperBase.Access.RO,
             "valid": ecwb.WrapperBase.Access.RW,
             "write_a": ecwb.WrapperBase.Access.RW,
             "unset": ecwb.WrapperBase.Access.WORM,
             "write_local_dt": ecwb.WrapperBase.Access.WORM}

    enums = {'a': TestEnum, 'b': TestEnum, 'write_a': TestEnum}
    geojson = []
    nullable = ["unset"]
    local_dates = ['write_local_dt']

    def _populateDependencies(self):
        # Add new properties called "invalid" and "valid" 
        # with values from the input
        # here, valid depends upon a and invalid depends upon b. Unfortunately, we cannot just do
        # self.valid = True because that call the current setattr, and that will
        # fail because dependent values are read-only. We can't even use the
        # set_attr method of super, since that is WrapperBase and WrapperBase
        # checks the "props" of the current class.  Instead, we call the
        # set_attr method of WrapperBase's parent, which has no checks.
        if "a" in self and self.a == TestEnum.B:
            super(ecwb.WrapperBase, self).__setattr__("valid", self.a)
        if "b" in self and self.b == TestEnum.C:
            super(ecwb.WrapperBase, self).__setattr__("invalid", self.b)

class TestBase(unittest.TestCase):
    def testCreationABC(self):
        test_tw = TestWrapper({'a': 1, 'b': 2, 'c': 3})
        self.assertEqual(test_tw.valid, TestEnum.B)
        self.assertEqual(test_tw.invalid, TestEnum.C)
        self.assertTrue(str(test_tw).startswith("TestWrapper"))

    def testCreationAB(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        self.assertEqual(test_tw.valid, TestEnum.B)
        with self.assertRaises(AttributeError):
            print ("test_tw.invalid = %s" % test_tw.invalid)
        self.assertTrue(str(test_tw).startswith("TestWrapper"))

    def testSetReadOnly(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        self.assertEqual(test_tw.valid, TestEnum.B)
        with self.assertRaisesRegex(AttributeError, ".*read-only.*"):
            test_tw.invalid = 2

    def testGetSetReadWrite(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        self.assertEqual(test_tw.valid, TestEnum.B)
        test_tw.valid = 2
        self.assertEqual(test_tw.valid, 2)

    def testSetEnumPositive(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        self.assertEqual(test_tw.valid, TestEnum.B)
        test_tw.write_a = TestEnum.C
        self.assertEqual(test_tw.write_a, TestEnum.C)
        self.assertEqual(test_tw["write_a"], 2)

    def testSetEnumNegative(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        self.assertEqual(test_tw.valid, TestEnum.B)
        with self.assertRaisesRegex(AttributeError, ".*enum.*"):
            test_tw.write_a = 2

    def testSetInvalid(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        with self.assertRaisesRegex(AttributeError, ".*not defined.*"):
            self.assertEqual(test_tw.z, 1)

    def testGetReadOnly(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        self.assertEqual(test_tw.a, TestEnum.B)

    def testGetInvalid(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        with self.assertRaisesRegex(AttributeError, ".*not defined.*"):
            self.assertEqual(test_tw.z, 1)

    def testIPythonAutoComplete(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        attributes = dir(test_tw)
        self.assertIn("a", attributes)
        self.assertIn("c", attributes)
        self.assertIn("valid", attributes)
        self.assertIn("invalid", attributes)
        self.assertIn("b", attributes)

    def testFirstTimeWrite(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        # This was originally unset and now we are setting it for the first time, so the write
        # succeeds
        test_tw.unset = 4
        # Now that it is set, it cannot be changed since it is read-only, so an
        # attempt to change it causes an exception
        with self.assertRaisesRegex(AttributeError, ".*read-only.*"):
            test_tw.unset = 5

    def testNullable(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        # this is nullable, so returns none if it is not set
        self.assertIsNone(test_tw.unset)
        # this is not nullable, so throws if not set
        with self.assertRaisesRegex(AttributeError, ".*has no attribute.*"):
            print("the value of b is %s" % test_tw.b)

    # The nested classes are hard to test because they load the wrappers automatically
    # from the wrapper directory, and so in order to test them, we either need to:
    # - use a module that is already in wrapper, OR
    # - create a new test module in wrapper
    # Trying to use WrapperBase for now to test. If that doesn't work, we will
    # switch to something else once we really have it.

    def testNestedClass(self):
        test_tw = TestWrapper({'a': 1, 'c': 3, 'WrapperBase': {'a': 11, 'c': 13}})
        # self.assertEqual(test_tw.WrapperBase.a, 11)

    def testLocalDate(self):
        import emission.core.wrapper.localdate as ecwl

        test_local = TestWrapper({'a': 1, 'c': 3})
        test_local.write_local_dt = ecwl.LocalDate({'year': 2016, 'month': 4})
        self.assertEqual(test_local.write_local_dt.year, 2016)
        self.assertEqual(test_local.write_local_dt.month, 4)
        with self.assertRaisesRegex(AttributeError, ".*has no attribute.*"):
            print("the value of day is %s" % test_local.write_local_dt.day)

if __name__ == '__main__':
    unittest.main()

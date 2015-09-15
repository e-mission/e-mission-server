# Test the base class enhancements over AttrDict
# Since the base class should not really contain any properties, we create a
# dummy subclass here and use it for testing.

# Standard imports
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
             "write_a": ecwb.WrapperBase.Access.RW}

    enums = {'a': TestEnum, 'b': TestEnum, 'write_a': TestEnum}

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
        self.assertEquals(test_tw.valid, TestEnum.B)
        self.assertEquals(test_tw.invalid, TestEnum.C)
        self.assertTrue(str(test_tw).startswith("TestWrapper"))

    def testCreationAB(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        self.assertEquals(test_tw.valid, TestEnum.B)
        with self.assertRaises(AttributeError):
            print ("test_tw.invalid = %s" % test_tw.invalid)
        self.assertTrue(str(test_tw).startswith("TestWrapper"))

    def testSetReadOnly(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        self.assertEquals(test_tw.valid, TestEnum.B)
        with self.assertRaisesRegexp(AttributeError, ".*read-only.*"):
            test_tw.invalid = 2

    def testGetSetReadWrite(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        self.assertEquals(test_tw.valid, TestEnum.B)
        test_tw.valid = 2
        self.assertEquals(test_tw.valid, 2)

    def testSetEnumPositive(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        self.assertEquals(test_tw.valid, TestEnum.B)
        test_tw.write_a = TestEnum.C
        self.assertEquals(test_tw.write_a, TestEnum.C)

    def testSetEnumNegative(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        self.assertEquals(test_tw.valid, TestEnum.B)
        with self.assertRaisesRegexp(AttributeError, ".*enum.*"):
            test_tw.write_a = 2

    def testSetInvalid(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        with self.assertRaisesRegexp(AttributeError, ".*not defined.*"):
            self.assertEquals(test_tw.z, 1)

    def testGetReadOnly(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        self.assertEquals(test_tw.a, TestEnum.B)

    def testGetInvalid(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        with self.assertRaisesRegexp(AttributeError, ".*not defined.*"):
            self.assertEquals(test_tw.z, 1)

    def testIPythonAutoComplete(self):
        test_tw = TestWrapper({'a': 1, 'c': 3})
        attributes = dir(test_tw)
        self.assertIn("a", attributes)
        self.assertIn("c", attributes)
        self.assertIn("valid", attributes)
        self.assertIn("invalid", attributes)
        self.assertIn("b", attributes)

    # The nested classes are hard to test because they load the wrappers automatically
    # from the wrapper directory, and so in order to test them, we either need to:
    # - use a module that is already in wrapper, OR
    # - create a new test module in wrapper
    # Trying to use WrapperBase for now to test. If that doesn't work, we will
    # switch to something else once we really have it.

    def testNestedClass(self):
        test_tw = TestWrapper({'a': 1, 'c': 3, 'WrapperBase': {'a': 11, 'c': 13}})
        # self.assertEqual(test_tw.WrapperBase.a, 11)

if __name__ == '__main__':
    unittest.main()

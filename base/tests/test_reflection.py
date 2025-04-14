import unittest
from base.reflection import get_modules, get_classes
from base.tests.testmodule.testfile import A,B,C
from base.tests.testmodule.testfile2 import D
from base.tests.testmodule.innermodule.testinner import E

class TestReflection(unittest.TestCase):

    def test_get_modules(self):
        expect = ["base.tests.testmodule.testfile", "base.tests.testmodule.testfile2"]
        result = get_modules("base.tests.testmodule", recursive=False)
        self.assertEqual(sorted(expect), sorted(result))

        expect = [*expect, "base.tests.testmodule.innermodule.testinner"]
        result = get_modules("base.tests.testmodule", recursive=True, skip_folders={"skippedmodule"})
        self.assertEqual(sorted(expect), sorted(result))

    def test_get_classes(self):
        expect = [A,B,D]
        result = get_classes("base.tests.testmodule", recursive=False, base=A)
        self.assertEqual(set(expect), set(result))

        expect = [*expect, E]
        result = get_classes("base.tests.testmodule", recursive=True, skip_folders={"skippedmodule"}, base=A)
        self.assertEqual(set(expect), result)
    

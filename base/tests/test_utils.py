from unittest import TestCase
from base.utils import cached

class TestUtils(TestCase):
    def test_cached(self):
        class A:
            def __init__(self, x: int):
                self.x = x
            def __eq__(self, other):
                return type(self) == type(other) and self.x == other.x
            def __hash__(self):
                return hash(self.x)
            def __str__(self): return f"A({self.x})"
        invocations = 0
        @cached
        def get_something(a: A, b: int) -> list[str]:
            nonlocal invocations
            invocations += 1
            return [f"{a}-{b}"]

        t1 = get_something(A(1), 1)
        t2 = get_something(A(1), 2)
        t3 = get_something(A(1), 1)
        t4 = get_something(A(2), 1)
        self.assertEqual(t1, ["A(1)-1"])
        self.assertIs(t1,t3)
        self.assertNotEqual(t1, t2)
        self.assertNotEqual(t1, t4)
        self.assertEqual(3, invocations)

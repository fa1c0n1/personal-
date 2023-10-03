import unittest


class A(object):

    def targetA(self, param: dict):
        newItems = {}
        for key, value in param.items():
            newItems[key] = value
        return newItems


class MyTestCase(unittest.TestCase):

    def test_getattr(self):
        params = {'1': 1, '2': 2}
        attr = getattr(A(), 'targetA', params)
        updated_items = attr(params)
        self.assertTrue(params == updated_items)


if __name__ == '__main__':
    unittest.main()

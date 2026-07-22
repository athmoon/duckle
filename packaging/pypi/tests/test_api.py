import unittest

import duckle


class PipelineTests(unittest.TestCase):
    def test_limit_uses_engine_property_name(self):
        pipeline = duckle.read_csv("input.csv").limit(7)

        properties = pipeline.to_dict()["nodes"][-1]["data"]["properties"]

        self.assertEqual(properties, {"limit": 7})


if __name__ == "__main__":
    unittest.main()

import unittest


class MyTestCase(unittest.TestCase):
    spark = None
    reset = True

    def setUp(self):
        from pyspark.sql import SparkSession

        self.spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .master("local[2]") \
            .getOrCreate()

    def tearDown(self):
        self.spark.stop()


    def test_spark(self):
        from pyspark.sql import SparkSession

        spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .master("local[2]") \
            .getOrCreate()

        spark.read.option("header", "true").csv("../data/a.csv").show()

    def wrapper_out(file_path):
        def wrapper(func):
            def inner(self, *args, **kwargs):

                with open(file_path, mode='r') as f:
                    columns = []
                    types = []
                    values = []
                    for idx, line in enumerate(f):
                        if idx == 0:
                            columns = line.strip().split(",")
                            continue
                        if idx == 1:
                            types = line.strip().split(",")
                            continue
                        values.append(line.strip().split(","))

                self.spark.read.csv("/Users/andy/PycharmProjects/bigdata/data/a.csv").show()
                return func(self, *args, **kwargs)
            return inner

        return wrapper

    @wrapper_out('/Users/andy/PycharmProjects/bigdata/data/a.csv')
    def test_wechat(self):
        print('ssssss')

    def clothes(func):
        def ware(self, *args, **kwargs):
            print('This is a decrator!')
            if self.reset == True:
                print('Reset is Ture, change Func..')
                self.func = False
            else:
                print('reset is False.')

            self.spark.read.csv("/Users/andy/PycharmProjects/bigdata/data/a.csv").show()
            return func(self, *args, **kwargs)

        return ware

    @clothes
    def test_body(self):
        print('The body feels could!')

if __name__ == '__main__':
    unittest.main()

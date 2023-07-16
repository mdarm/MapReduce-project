from pyspark.sql import SparkSession
from utils import timeit


spark = SparkSession \
          .builder \
          .appName("RDD API") \
          .getOrCreate() \
          .sparkContext


employees   = spark.textFile("hdfs://master:9000/home/user/files/employeesR.csv")
departments = spark.textFile("hdfs://master:9000/home/user/files/departmentsR.csv")


def repartition_join():
    # Tag RDDs
    employees_tagged = employees \
                         .map(lambda line: line.split(',')) \
                         .map(lambda field: (int(field[0]), field[1], int(field[2]))) \
                         .map(lambda field: (field[2], ('employees', field)))

    departments_tagged = departments \
                           .map(lambda line: line.split(',')) \
                           .map(lambda field: (int(field[0]), field[1])) \
                           .map(lambda field: (field[0], ('departments', field)))

    # Concatenate all RDDS
    union = employees_tagged.union(departments_tagged)

    # Perform the join for each key
    def join_records(records):
        employees_records   = [field[1] for field in records if field[0] == 'employees']
        departments_records = [field[1] for field in records if field[0] == 'departments']
        return [(e[1], e[0], d[1]) for e in employees_records for d in departments_records]

    # Extract union result
    joined_rdd = union.groupByKey() \
                   .flatMap(lambda pair: join_records(pair[1]))

    return timeit(joined_rdd.collect)


def broadcast_join():
    employees_rdd = employees \
                      .map(lambda line: line.split(',')) \
                      .map(lambda field: (int(field[0]), field[1], int(field[2])))

    departments_rdd = departments \
                        .map(lambda line: line.split(',')) \
                        .map(lambda field: (int(field[0]), field[1]))

    # Create a dictionary for department data
    dep_dict = {field[0]: field[1] for field in departments_rdd.collect()}

    # Broadcast the department data
    dep_broadcast = spark.sparkContext.broadcast(dep_dict)

    def map_func(field):
        # Extract the join key (department ID) and the department name using
        # the broadcast variable
        dep_id = field[2]
        dep_name = dep_broadcast.value.get(dep_id)

        # Joined data
        return (field[1], field[0], dep_name)

    joined_rdd = employees_rdd.map(lambda field: map_func(field))

    return timeit(joined_rdd.collect)

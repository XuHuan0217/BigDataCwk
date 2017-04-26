
from pyspark import SparkContext, SparkConf, RDD
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession, Row
import json
from datetime import datetime

def getSparkSessionInstance(sparkConf: SparkConf) -> SparkSession:
    """ This method ensures that there is only ever one instance of the Spark
    SQL context. This prevents conflicts between executors.

    Code taken from example on this page of the Spark Streaming Guide:

    https://spark.apache.org/docs/latest/
    streaming-programming-guide.html#dataframe-and-sql-operations

    Arguments:
        sparkConf - SparkConf - The spark context configuration object.
        This should be the same as supplied to the StreamingContext object.

    Returns:
        The singleton instance of the Spark SQL SparkSession object.
    """
    if "sparkSessionSingletonInstance" not in globals():
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()

    return globals()["sparkSessionSingletonInstance"]

def send_to_cassandra(rdd: RDD, table_name: str) -> None:
    """ Converts an RDD of Row instances into a DataFrame and appends it to
    the supplied table name. The Cassandra database address is defined in the
    "spark.cassandra.connection.host" key set in the SparkConf instance given
    to the getSparkSessionInstance method.

    Arguments:
        rdd - RDD - An rdd of Row instances with filed names that match those
        in the target cassandra table.
        table_name - str - The name of the table that this rdd should be sent
        to.

    """
    # Get the singleton instance of SparkSession
    spark = getSparkSessionInstance(rdd.context.getConf())

    # Convert the supplied rdd of Rows into a dataframe
    rdd_df = spark.createDataFrame(rdd)

    # Print the dataframe to the console for verification
    rdd_df.show()

    # Write this dataframe to the supplied Cassandra table
    (rdd_df.write.format("org.apache.spark.sql.cassandra").mode('append')
     .options(table=table_name, keyspace="csc8101").save())

########## Spark Streaming Setup ##########

# Create a conf object to hold connection information
sp_conf = SparkConf()

# Set the cassandra host address - NOTE: you will have to set this to the
# address of your VM if you run this on the cluster. If you are running
# the streaming job locally on your VM you should set this to "localhost"
sp_conf.set("spark.cassandra.connection.host", "localhost")

# Create the spark context object.
# For local development it is very important to use "local[2]" as Spark
# Streaming needs 2 cores minimum to function properly
SC = SparkContext(master="local[2]", appName="Event Processing", conf=sp_conf)

#Set the batch interval in seconds
BATCH_INTERVAL = 10

#Create the streaming context object
SSC = StreamingContext(SC, BATCH_INTERVAL)

########## Kafka Setup ##########

# Create receiver based stream
# NOTE: Please edit the Broker IP to match your designated broker address.
# Also edit the client_id_for_broker to be something personal to you such as
# your student number. This is used for the broker to identify your client.
# Also edit the topic_name you would like to pull messages from and the number
# of partitions to consume on that topic. For development 1 is fine, for 
# consuming from the production stream a higher number is recommended.
topic_name = "dev-stream"
client_id_for_broker = "160204137"
num_of_partitions_to_consume_from = 1
RAW_MESSAGES = KafkaUtils.createStream(SSC,
                                       "34.249.15.212:2181",
                                       client_id_for_broker,
                                       {topic_name: num_of_partitions_to_consume_from})

########## Window the incoming batches ##########
window_ds = RAW_MESSAGES.window(60,30)


########## Convert each message from json into a dictionary ##########
window_ds = window_ds.map(lambda line: json.loads(line[1]))
window_ds.pprint(2)
########## Find the minimum timestamp for window ##########

min_time = window_ds.map(lambda line:int(datetime.strptime(line['timestamp'], "%Y-%m-%dT%H:%M:%S").timestamp()*1e3)).reduce(lambda a,b: a if a<b else b)
min_time=min_time.map(lambda line:(None,line))
min_time.pprint()


############################## Task 1 ##############################

#### Calculate the page visits for each client id ####
clients_visit = window_ds.map(lambda line:((line['client_id'],line['url']['topic'],line['url']['page']),1)).reduceByKey(lambda x,y:x+y)

clients_visit = clients_visit.map(lambda line: (None,(line[0],line[1]))).join(min_time)
#(None, ((('1615202', 'Action', 'Forrest Gump'), 1), 1488317609000))
# 0     100          1         2                1   1 
#client id ,batchtime, topic ,page,count
clients_visit = clients_visit.map(lambda line: (line[1][0][0][0],line[1][1],line[1][0][0][1],line[1][0][0][2],line[1][0][1]))
clients_visit.pprint(2)

#### Write the Task 1 results to cassandra ####

# Convert each element of the rdd_example rdd into a Row
# NOTE: This is just example code please rename the DStreams and Row fields to
# match those in your Cassandra table

# Convert each rdd of Rows in the DStream into a DataFrame and send to Cassandra



############################## Task 2 ##############################

#### Calculate the unique views per page
# batchtime topic page , client id->count
distinct_user = clients_visit.map(lambda line: ((line[1],line[2],line[3]),line[0])).combineByKey(lambda v: set([v]),lambda v,c : v.union([c]),lambda c1,c2 : c1.union(c2)).map(lambda line: (line[0][0],line[0][1],line[0][2],len(line[1])))
distinct_user.pprint(2)
#### Write the Task 2 results to cassandra ####
row_rdd_t1 = clients_visit.map(lambda x: Row(batchtime=x[1],
                                                clientid=x[0],
                                                topic=x[2],
                                                page=x[3],
                                                count=x[4]))
row_rdd_t3 = clients_visit.map(lambda x:Row(clientid=x[0],batchtime=x[1]))
row_rdd_t2 = distinct_user.map(lambda x:Row(topic=x[1],batchtime=x[0],count=x[3],page=x[2]))
row_rdd_t1.foreachRDD(lambda rdd: send_to_cassandra(rdd, 'visits_per_url'))
row_rdd_t3.foreachRDD(lambda rdd: send_to_cassandra(rdd,'user_batchtime'))
row_rdd_t2.foreachRDD(lambda rdd: send_to_cassandra(rdd,'distinct_user_counts'))
# Initirow_rdd_t3.foreachRDD(lambda rdd: send_to_cassandraate the stream processing
SSC.start()
SSC.awaitTermination()
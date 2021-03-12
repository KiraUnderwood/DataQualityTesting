import csv
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from hdfs import InsecureClient
from hdfs3 import HDFile, HDFileSystem, HDFSMap
from snakebite import client


'''
i've tried many approaches but neither of them worked
so please fix/update your VM to supported version and provide normal guidances
 for this task so that ppl do not waste their life on this
'''

client_hdfs = HDFileSystem(host='localhost', port=8020)


# client_hdfs = InsecureClient('http://localhost:50020', user='cloudera')
#client_hdfs.list('/user/student/airlines/')

with client_hdfs.open(r'/user/student/airlines/airlines.dat') as f:
                reader = csv.DictReader(f)
                schema = avro.schema.parse(open("/home/cloudera/Desktop/data/shema.json", "rb").read())
                with client_hdfs.write('/user/student/airlines/airlines.avro') as out:
                        writer = DataFileWriter(out, DatumWriter(), schema)
                        for line in reader:
                                writer.append(line)
                        writer.close()


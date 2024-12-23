import datetime
import json
import os
import uuid
from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

class Config:

	_instance = None

	def __new__(cls, filename='general.json'):
		if cls._instance is None:
			cls._instance = super(Config, cls).__new__(cls)
			cls._instance._load(filename)
		return cls._instance

	def __getattr__(self, name):
		if name in self._settings.__dict__:
			return self._settings.__dict__[name]
		raise AttributeError(f"'Config' object has no attribute '{name}'")

	def _load(self, filename):
		path = os.path.join(os.path.dirname(__file__), 'settings/'+filename)
		if not os.path.exists(path):
			raise FileNotFoundError(f"Configuration file not found: {path}")
		with open(path, 'r') as file:
			self._settings = json.load(file)
		self._settings = self._to_obj(self._settings)

	def _to_obj(self, dictionary):
		if not isinstance(dictionary, dict):
			return dictionary
		return type('ConfigObject', (object,), {k: self._to_obj(v) for k, v in dictionary.items()})


class Worker():

	def __init__(self):
		self.spark = SparkSession.builder \
			.appName("arm_delta") \
			.config("spark.jars.packages", Config().delta) \
			.config("spark.driver.cleanupOnShutdown", "false") \
			.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
			.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
			.getOrCreate()
	    # pass

	def load_schema(self, filename):
		path = os.path.join(os.path.dirname(__file__), 'schema/'+filename+'.json')
		if not os.path.exists(path):
			raise FileNotFoundError(f"Schema file not found: {path}")
		with open(path, 'r') as file:
			schema = json.load(file)
			fields = schema['fields']
			track = schema['track']
		return (StructType.fromJson({"type": "struct", "fields": fields}), track)

	def record(self, data):

		schema = self.load_schema(Config().tables.bimcloud25.schema)
		table_path = Config().storage + '/' + Config().tables.bimcloud25.name

		# Try to load the Delta table
		try:
		    existing_df = self.spark.read.format("delta").load(table_path)
		except:
		    # Initialize an empty DataFrame if the table doesn't exist
		    existing_df = self.spark.createDataFrame([], schema[0])

		existing_df.createOrReplaceTempView("delta_table")

		# Convert to DataFrame
		new_data_df = self.spark.createDataFrame(
		    [(uuid.uuid4().hex, datetime.datetime.now().isoformat(),  datetime.datetime.now().strftime("%Y-%m"), *record) for record in data],
		    schema=schema[0]
		)

		# Detect Changes (Anti-Join)
		key_columns = schema[1]
		changed_data = new_data_df.join(
		    existing_df,
		    [new_data_df[c] == existing_df[c] for c in key_columns],
		    "left_anti"
		)

		# Write Only Changed Data to Delta Table
		if changed_data.count() > 0:
		    changed_data.write.format("delta") \
		        .partitionBy("file_id", "_ym") \
		        .mode("append") \
		        .save(table_path)

		self.spark.stop()

if __name__ == "__main__":

	worker = Worker()

	data = [
	    ("df3ws3", "file1", "prj1", "project", 2345, "25.1", "active", 1734882835, 1734882835, 1734882835, 1734882835, 500, 5, 1 ),
	    ("as34gv", "file2", "prj2", "project", 3120, "25.1", "active", 1734882835, 1734882835, 1734882835, 1734882835, 400, 3, 1 ),
	    ("56h3sk", "file3", "prj1", "project", 2783, "25.1", "active", 1734882835, 1734882835, 1734882835, 1734882835, 700, 1, 0),
	]

	worker.record(data)
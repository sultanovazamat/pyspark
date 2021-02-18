import os
import argparse
import subprocess
import psycopg2
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from glob import glob

class OSM:
    
    def process_txt(self, spark, prefix, filename, parquet_name):
        # read file to rdd
        rdd = spark.sparkContext.textFile("file:///" + prefix + filename)
        # get headers
        initial_header = rdd.take(1)[0]
        final_header = initial_header
        final_header = final_header.split(";")
        final_header = [final_header[5], "PAR_OSM_ID", final_header[0], final_header[3]]  
        # remove headers from rdd
        rdd_no_header = rdd.filter(lambda l: l != initial_header)
        # process rdd lines
        rdd_processed = rdd_no_header.map(lambda l: self.process_rdd_row(l, initial_header))
        # generate dataframe
        df = self.generate_df(rdd_processed, final_header)
        # append df to the final parquet
        self.write_df_to_parquet(df, parquet_name)
        
    def process_rdd_row(self, line, header):
        cols = header.split(";")
        vals = line.split(";")
        ADMIN_LVL = int(vals[cols.index("ADMIN_LVL")])
        i = 1
        while True:
            if ADMIN_LVL == i:
                vals.append("0")
                break
            if vals[cols.index(f"ADMIN_L{ADMIN_LVL - i}D")] in ["None", ""]:
                i += 1
            else:
                vals.append(str(int(float(vals[cols.index(f"ADMIN_L{ADMIN_LVL - i}D")]))))
                break
                
        return ";".join([vals[5], vals[-1], vals[0], vals[3]])
    
    def generate_df_row(self, vals, cols):
        d = {}
        for col, val in zip(cols, vals.split(";")):
            d[col] = val
            
        return d
    
    def generate_df(self, rdd, header):
        return rdd.map(lambda l: Row(**self.generate_df_row(l, header))).toDF()
    
    def remove_old_parquet(self, parquet_name):
        try:
            shutil.rmtree(parquet_name)
        except:
            pass
    
    def write_df_to_parquet(self, df, parquet_name):
        df.write.mode("append").parquet(parquet_name)

class OSMDB:
    
    def __init__(self):
        self.conn = psycopg2.connect(
           database="osmdb", user='postgres', password='password', host='127.0.0.1', port='5432'
        )
        self.cursor = self.conn.cursor()
        self.create_table()

    
    def execute_and_commit(self, sql):
        try:
           # Executing the SQL command
           self.cursor.execute(sql)

           # Commit your changes in the database
           self.conn.commit()

        except Exception as e:
            print(e.message)
           # Rolling back in case of error
            self.conn.rollback()
    
    
    def create_table(self):
        sql ='''CREATE TABLE IF NOT EXISTS OSM(
           OSM_ID VARCHAR(20) PRIMARY KEY,
           PAR_OSM_ID VARCHAR(20),
           NAME VARCHAR(255),
           ADMIN_LVL VARCHAR(5)
        )'''
        
        self.execute_and_commit(sql)
        
        
    def insert_data(self, data):
        for row in data:
            row_dict = row.asDict()
            sql = "INSERT INTO OSM ("
            keys = list(row_dict.keys())
            for i in range(len(keys)):
                sql += keys[i]
                if i < len(row_dict) - 1:
                    sql += ","
            sql += ") VALUES ("
            vals = list(row_dict.values())
            for i in range(len(vals)):
                sql += "'" + vals[i] + "'"
                if i < len(row_dict) - 1:
                    sql += ","
            sql += ") ON CONFLICT DO NOTHING"
            self.execute_and_commit(sql)
            
    
    def read_data(self):
        sql = "SELECT * FROM OSM ORDER BY ADMIN_LVL"
        self.execute_and_commit(sql)
        result = self.cursor.fetchall()
        print(f"Selected {len(result)} rows\n\nFirst 10 rows:")
        for i in result[:10]:
            print(i)
        print("\nLast 10 rows:")
        for i in result[-10:]:
            print(i)
    
    
    def close(self):
        self.cursor.close()
        self.conn.close()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", help='path to folder containing files to process, e.g. "./data/*.txt"', required=True, type=str)
    parser.add_argument("--verbose", help="print detailed output", action="store_true")
    args = parser.parse_args()

    prefix = os.path.abspath(".")

    spark = SparkSession.builder.master("local[*]").appName('OSM').getOrCreate()

    to_process = sorted(glob(args.input))

    osm = OSM()
    osmdb = OSMDB()
    parquet_name = "processed-osm.parquet"

    osm.remove_old_parquet(parquet_name)
    for i in to_process:
        osm.process_txt(spark, prefix, i[1:], parquet_name)

    df = spark.read.parquet("processed-osm.parquet")
    
    if args.verbose:
        print(df.toPandas().head(10))
        print()

    osmdb.insert_data(df.rdd.collect())
    
    if args.verbose:
        osmdb.read_data()
        print()
        subprocess.run('ls processed-osm.parquet/*.parquet', shell=True)

if __name__ == "__main__":
    main()
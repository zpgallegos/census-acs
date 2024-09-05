import os
import pandas as pd

from pyspark.sql import SparkSession, functions as F, Window as W

DATA_DIR = "data/pums-2022-1yr"

venv = "powerlytics"

os.environ["SPARK_HOME"] = (
    f"/Users/zpgallegos/opt/anaconda3/envs/{venv}/lib/python3.12/site-packages/pyspark"
)


def load_data(data_dir: str, *args, **kwargs) -> pd.DataFrame:
    """
    Load the PUMS data from the local directory
    """
    dfs = []
    for i, file in enumerate(os.listdir(data_dir)):
        df = pd.read_csv(os.path.join(data_dir, file), *args, **kwargs)
        dfs.append(df)

        if not i:
            header = list(df.columns)
        else:
            assert list(df.columns) == header, "mismatching columns between files"

    return pd.concat(dfs, axis=0)


if __name__ == "__main__":

    # d = load_data(DATA_DIR)

    spark = SparkSession.builder.appName("pums").getOrCreate()
    schema = open("ipums_2022_1year-schema.txt").read()

    df = spark.read.format("csv").schema(schema).load(DATA_DIR)

    # find any columns that are completely missing to test schema validity
    n = df.count()
    missing_cols = []
    for i, col in enumerate(df.columns):
        if i and i % 10 == 0:
            print(f"{i}/{len(df.columns)}")
        if df.where(F.col(col).isNull()).count() == n:
            missing_cols.append(col)
            print(col)
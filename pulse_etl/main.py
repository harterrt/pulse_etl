from datetime import *
import dateutil.parser
from pyspark.sql.types import *
from pyspark.sql import Row

from moztelemetry import get_pings_properties
from moztelemetry.dataset import Dataset


class ColumnConfig:
    def __init__(self, name, path, cleaning_func, struct_type):
        self.name = name
        self.path = path
        self.cleaning_func = cleaning_func
        self.struct_type = struct_type

class DataFrameConfig:
    def __init__(self, col_configs):
        self.columns = [ColumnConfig(*col) for col in col_configs]

    def toStructType(self):
        return StructType(map(
            lambda col: StructField(col.name, col.struct_type, True),
            self.columns))

    def get_names(self):
        return map(lambda col: col.name, self.columns)

    def get_paths(self):
        return map(lambda col: col.path, self.columns)



def pings_to_df(sqlContext, pings, data_frame_config):
    """Performs simple data pipelining on raw pings

    Arguments:
        data_frame_config: a list of tuples of the form:
                 (name, path, cleaning_func, column_type)
    """
    def build_cell(ping, column_config):
        """Takes a json ping and a column config and returns a cleaned cell"""
        raw_value = ping[column_config.path]
        func = column_config.cleaning_func
        if func is not None:
            try:
                return func(raw_value)
            except:
                return None
        else:
            return raw_value

    def ping_to_row(ping):
        return [build_cell(ping, col) for col in data_frame_config.columns]

    filtered_pings = get_pings_properties(pings, data_frame_config.get_paths())

    return sqlContext.createDataFrame(
        filtered_pings.map(ping_to_row),
        schema = data_frame_config.toStructType())

class Request:
    def __option__(func):
        return lambda x: func(x) if x is not None else None

    int_type = (__option__(int), LongType())
    float_type = (__option__(float), DoubleType())

    field_types = {
        'num': int_type,
        'cached': float_type,
        'cdn': float_type,
        'time': int_type,
    }

    StructType = StructType([
        StructField(key, field_types[key][1], True) for key in field_types
    ])

    def __init__(self, request_dict):
        args = {field: conversion(request_dict.get(field))
                for field, (conversion, sql_type) in Request.field_types.items()}
        self.Row = Row(**args)



def transform_pings(sqlContext, pings):
    def requests_to_rows(requests):
        out =  {k: Request(v).Row for k, v in requests.items()}
        return out

    RequestsType = MapType(StringType(), Request.StructType)

    return pings_to_df(
        sqlContext,
        pings,
        DataFrameConfig([
            ("method", "payload/payload/method", None, StringType()),
            ("id", "payload/payload/id", None, StringType()),
            ("type", "payload/payload/type", None, StringType()),
            ("object", "payload/payload/object", None, StringType()),
            ("category", "payload/payload/category", None, StringType()),
            ("variant", "payload/payload/variant", None, StringType()),
            ("details", "payload/payload/details", None, StringType()),
            ("sentiment", "payload/payload/sentiment", None, IntegerType()),
            ("reason", "payload/payload/reason", None, StringType()),
            ("adBlocker", "payload/payload/adBlocker", None, BooleanType()),
            ("addons", "payload/payload/addons", None, ArrayType(StringType())),
            ("channel", "payload/payload/channel", None, StringType()),
            ("hostname", "payload/payload/hostname", None, StringType()),
            ("language", "payload/payload/language", None, StringType()),
            ("openTabs", "payload/payload/openTabs", None, IntegerType()),
            ("openWindows", "payload/payload/openWindows", None, IntegerType()),
            ("platform", "payload/payload/platform", None, StringType()),
            ("protocol", "payload/payload/protocol", None, StringType()),
            ("telemetryId", "payload/payload/telemetryId", None, StringType()),
            ("timerContentLoaded", "payload/payload/timerContentLoaded", None, LongType()),
            ("timerFirstInteraction", "payload/payload/timerFirstInteraction", None, LongType()),
            ("timerFirstPaint", "payload/payload/timerFirstPaint", None, LongType()),
            ("timerWindowLoad", "payload/payload/timerWindowLoad", None, LongType()),
            ("inner_timestamp", "payload/payload/timestamp", None, LongType()),
            ("fx_version", "payload/payload/fx_version", None, StringType()),
            ("creation_date", "creationDate", dateutil.parser.parse, TimestampType()),
            ("test", "payload/test", None, StringType()),
            ("variants", "payload/variants", None, StringType()),
            ("timestamp", "payload/timestamp", None, LongType()),
            ("version", "payload/version", None, StringType()),
            ("requests", "payload/payload/requests", requests_to_rows, RequestsType)
        ])).filter("test = 'pulse@mozilla.com'")


def etl_job(sc, sqlContext, submission_date=None, save=True):
    if submission_date is None:
        submission_date = (date.today() - timedelta(1)).strftime("%Y%m%d")

    pings = Dataset.from_source("telemetry")\
                    .where(docType="testpilottest")\
                    .where(submissionDate=submission_date)\
                    .where(appName="Firefox")\
                    .records(sc)

    tpt = transform_pings(sqlContext, pings)

    if save:
        path = 's3://telemetry-parquet/testpilot/txp_pulse/v1/submission_date={}'
        tpt.repartition(1).write.mode('overwrite')\
            .parquet(path.format(submission_date))

    return tpt


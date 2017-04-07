import pytest
import json
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pulse_etl.main import transform_pings

# Initialize a spark context:
@pytest.fixture(scope="session")
def spark_context(request):
    conf = SparkConf().setMaster("local")\
        .setAppName("pulse_etl" + "_test")
    sc = SparkContext(conf=conf)

    # teardown
    request.addfinalizer(lambda: sc.stop())
    
    return sc

# Generate some data
def create_row():
    with open('tests/example_ping.json') as infile:
        return json.load(infile)

@pytest.fixture
def simple_rdd(spark_context):
    return spark_context.parallelize([create_row()])

# Tests
def test_simple_transform(simple_rdd, spark_context):
    actual = transform_pings(SQLContext(spark_context), simple_rdd).take(1)[0]
    expected = {
        'method': None,
        'id': u'19e0cf07-8145-4666-938d-811397db85dc',
        'type': None,
        'object': None,
        'category': None,
        'variant': None,
        'details': u'test',
        'sentiment': 5,
        'reason': u'like',
        'adBlocker': True,
        'addons': [u'pulse@mozilla.com', u'inspector@mozilla.org',
                u'DevPrefs@jetpack', u'@min-vid'],
        'channel': u'developer',
        'hostname': u'github.com',
        'language': u'en-US',
        'openTabs': 7,
        'openWindows': 2,
        'platform': u'darwin',
        'protocol': u'https:',
        'telemetryId': u'549a6456-32c2-e048-8310-d22ccfa9fee1',
        'timerContentLoaded': 589,
        'timerFirstInteraction': None,
        'timerFirstPaint': 41,
        'timerWindowLoad': 1005,
        'inner_timestamp': 1487862372503,
        'fx_version': None,
        'creation_date': None,
        'test': u'pulse@mozilla.com',
        'variants': None,
        'timestamp': 76543,
        'version': u'1.0.2'
    }

    assert all(map(lambda key: actual[key] == expected[key], expected.keys()))


# Importing dependencies
from google.protobuf.duration_pb2 import Duration
from feast import Entity, Feature, FeatureView, FileSource, ValueType

# Declaring an entity for the dataset
patient = Entity(
    name="driver_id", 
    value_type=ValueType.INT64, 
    description="The ID of the driver"
    )

# Declaring the source for raw feature data
file_source = FileSource(
    path=r"C:\feast\driver_stats\data\driver_stats_with_string.parquet",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created"
)

# Defining the features in a feature view
driver_stats_fv = FeatureView(
    name="driver_stats_fv",
    ttl=Duration(seconds=86400 * 2),
    entities=["driver_id"],
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.FLOAT)        
        ],    
    batch_source=file_source
)
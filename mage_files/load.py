from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def export_dataframe_to_bigquery(df: DataFrame, table_id: str, config_loader) -> None:
    """
    Helper function to export a single DataFrame to BigQuery.
    """
    BigQuery.with_config(config_loader).export(
        df,
        table_id,
        if_exists='replace',  # Specify resolution policy if table name already exists
    )


@data_exporter
def export_data_to_big_query(output: dict, **kwargs) -> None:
    """
    Exports each DataFrame in the output dictionary to a corresponding table in BigQuery.
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    config_loader = ConfigFileLoader(config_path, config_profile)

    # Mapping of DataFrame keys to their respective BigQuery table names
    table_mapping = {
        'datetime_dim': 'uber-data-analysis-431809.uber_de_engg.datetime_dim',
        'passenger_count_dim': 'uber-data-analysis-431809.uber_de_engg.passenger_count_dim',
        'trip_distance_dim': 'uber-data-analysis-431809.uber_de_engg.trip_distance_dim',
        'rate_code_dim': 'uber-data-analysis-431809.uber_de_engg.rate_code_dim',
        'pickup_location_dim': 'uber-data-analysis-431809.uber_de_engg.pickup_location_dim',
        'dropoff_location_dim': 'uber-data-analysis-431809.uber_de_engg.dropoff_location_dim',
        'payment_type_dim': 'uber-data-analysis-431809.uber_de_engg.payment_type_dim',
        'fact_table': 'uber-data-analysis-431809.uber_de_engg.fact_table'
    }

    # Export each DataFrame to its respective table
    for key, table_id in table_mapping.items():
        df = output.get(key)
        if df is not None:
            export_dataframe_to_bigquery(df, table_id, config_loader)
    
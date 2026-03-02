import json
from datetime import datetime, timezone
import os

from google.cloud import bigquery
import great_expectations as gx

PROJECT_ID = "tpiuo-labosi"
DBT_DATASET = "stackoverflow_dbt"

BRONZE_TABLE = f"{PROJECT_ID}.{DBT_DATASET}.stackoverflow_questions_bronze"
SILVER_TABLE = f"{PROJECT_ID}.{DBT_DATASET}.stackoverflow_questions_silver"

def bq_to_df(sql: str):
    client = bigquery.Client(project=PROJECT_ID)
    return client.query(sql).to_dataframe()

def ensure_context_file_mode():
    return gx.get_context(mode="file")

def add_df_pipeline_objects(context, data_source_name, data_asset_name, batch_def_name):
    try:
        ds = context.data_sources.get(data_source_name)
    except Exception:
        ds = context.data_sources.add_pandas(name=data_source_name)

    try:
        asset = ds.get_asset(data_asset_name)
    except Exception:
        asset = ds.add_dataframe_asset(name=data_asset_name)

    try:
        batch_def = asset.get_batch_definition(batch_def_name)
    except Exception:
        batch_def = asset.add_batch_definition_whole_dataframe(batch_def_name)

    return batch_def

def make_or_get_suite(context, suite_name):
    try:
        return context.suites.get(suite_name)
    except Exception:
        suite = gx.ExpectationSuite(name=suite_name)
        return context.suites.add(suite)

def add_expectations(suite, table_kind: str):
    ex = gx.expectations

    suite.add_expectation(ex.ExpectColumnToExist(column="question_id"))
    suite.add_expectation(ex.ExpectColumnValuesToNotBeNull(column="question_id"))

    if table_kind == "bronze":
        suite.add_expectation(ex.ExpectColumnToExist(column="creation_date"))
        suite.add_expectation(ex.ExpectColumnValuesToNotBeNull(column="creation_date"))
        suite.add_expectation(ex.ExpectColumnToExist(column="title"))
        suite.add_expectation(ex.ExpectColumnValuesToNotBeNull(column="title"))
        suite.add_expectation(ex.ExpectTableRowCountToBeBetween(min_value=1, max_value=10_000_000))

    if table_kind == "silver":
        suite.add_expectation(ex.ExpectColumnToExist(column="created_ts"))
        suite.add_expectation(ex.ExpectColumnValuesToNotBeNull(column="created_ts"))

        suite.add_expectation(ex.ExpectColumnToExist(column="sentiment"))
        suite.add_expectation(
            ex.ExpectColumnValuesToBeInSet(
                column="sentiment",
                value_set=["HIGH_ENGAGEMENT", "POSITIVE", "NEGATIVE", "NEUTRAL"],
            )
        )

        suite.add_expectation(ex.ExpectColumnToExist(column="is_closed"))
        suite.add_expectation(ex.ExpectColumnValuesToNotBeNull(column="is_closed"))

    return suite

def run_validation(batch_def, suite, validation_name, df):
    vdef = gx.ValidationDefinition(data=batch_def, suite=suite, name=validation_name)
    return vdef.run(batch_parameters={"dataframe": df})

def save_result(result, out_path):
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result.to_json_dict(), f, ensure_ascii=False, indent=2)

def main():
    context = ensure_context_file_mode()

    bronze_df = bq_to_df(f"SELECT * FROM `{BRONZE_TABLE}`")
    silver_df = bq_to_df(f"SELECT * FROM `{SILVER_TABLE}`")

    bronze_batch_def = add_df_pipeline_objects(context, "so_pandas", "bronze_questions", "whole_df_bronze")
    silver_batch_def = add_df_pipeline_objects(context, "so_pandas", "silver_questions", "whole_df_silver")

    bronze_suite = make_or_get_suite(context, "stackoverflow_bronze_suite")
    silver_suite = make_or_get_suite(context, "stackoverflow_silver_suite")

    add_expectations(bronze_suite, "bronze")
    add_expectations(silver_suite, "silver")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    bronze_res = run_validation(bronze_batch_def, bronze_suite, f"bronze_validation_{ts}", bronze_df)
    silver_res = run_validation(silver_batch_def, silver_suite, f"silver_validation_{ts}", silver_df)

    out_dir = "gx_results"
    os.makedirs(out_dir, exist_ok=True)

    save_result(bronze_res, f"{out_dir}/bronze_validation_{ts}.json")
    save_result(silver_res, f"{out_dir}/silver_validation_{ts}.json")

    print("BRONZE success:", bronze_res.success)
    print("SILVER success:", silver_res.success)
    print("Saved JSON results to:", out_dir)

if __name__ == "__main__":
    main()

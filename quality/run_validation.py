import os
from pathlib import Path

from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import FileDataContext


CONTEXT_ROOT = Path("/workspace/quality/great_expectations")
CONTEXT_ROOT.mkdir(parents=True, exist_ok=True)


def ensure_context() -> FileDataContext:
    config_path = CONTEXT_ROOT / "great_expectations.yml"
    if not config_path.exists():
        config_path.write_text(
            """
config_version: 3.0
plugins_directory: plugins/
config_variables_file_path: uncommitted/config_variables.yml
stores:
  expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: expectations/
  validations_store:
    class_name: ValidationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/validations/
  checkpoint_store:
    class_name: CheckpointStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      suppress_store_backend_id: true
      base_directory: checkpoints/
  profiler_store:
    class_name: ProfilerStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: profilers/
  evaluation_parameter_store:
    class_name: EvaluationParameterStore
expectations_store_name: expectations_store
validations_store_name: validations_store
evaluation_parameter_store_name: evaluation_parameter_store
checkpoint_store_name: checkpoint_store
data_docs_sites:
  local_site:
    class_name: SiteBuilder
    show_how_to_buttons: false
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/data_docs/local_site/
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
anonymous_usage_statistics:
  enabled: false
""".strip(),
            encoding="utf-8",
        )
        (CONTEXT_ROOT / "expectations").mkdir(parents=True, exist_ok=True)
        (CONTEXT_ROOT / "checkpoints").mkdir(parents=True, exist_ok=True)
        (CONTEXT_ROOT / "profilers").mkdir(parents=True, exist_ok=True)
        (CONTEXT_ROOT / "plugins").mkdir(parents=True, exist_ok=True)
        (CONTEXT_ROOT / "uncommitted" / "validations").mkdir(parents=True, exist_ok=True)
        (CONTEXT_ROOT / "uncommitted" / "data_docs" / "local_site").mkdir(
            parents=True, exist_ok=True
        )

    return FileDataContext(context_root_dir=str(CONTEXT_ROOT))


def main() -> None:
    context = ensure_context()
    connection_string = os.getenv("TRINO_SQLALCHEMY_URL", "trino://trino@trino:8080/iceberg")
    datasource_name = "trino_lakehouse"

    context.add_or_update_datasource(
        name=datasource_name,
        class_name="Datasource",
        execution_engine={
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": connection_string,
        },
        data_connectors={
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["run_id"],
            }
        },
    )

    validation_specs = [
        {
            "suite_name": "gold_commerce_kpis_suite",
            "asset_name": "gold_commerce_kpis_1m",
            "query": """
                SELECT *
                FROM gold.commerce_kpis_1m
                WHERE window_start >= current_timestamp - INTERVAL '1' DAY
                  AND window_start IS NOT NULL
                  AND metric_name IN ('orders_created', 'gross_order_value', 'payments_succeeded', 'refund_amount')
                  AND metric_value >= 0
            """,
            "configure": lambda validator: validator.expect_table_row_count_to_be_between(
                min_value=1
            ),
        },
        {
            "suite_name": "gold_crypto_market_kpis_suite",
            "asset_name": "gold_crypto_market_kpis_1m",
            "query": """
                SELECT *
                FROM gold.crypto_market_kpis_1m
                WHERE window_start >= current_timestamp - INTERVAL '1' DAY
                  AND window_start IS NOT NULL
                  AND symbol IS NOT NULL
                  AND metric_name IN (
                    'tick_count_1m',
                    'traded_volume_1m',
                    'traded_notional_1m',
                    'vwap_1m',
                    'price_volatility_1m'
                  )
                  AND metric_value >= 0
            """,
            "configure": lambda validator: validator.expect_table_row_count_to_be_between(
                min_value=1
            ),
        },
    ]

    validations = []
    for spec in validation_specs:
        try:
            context.get_expectation_suite(expectation_suite_name=spec["suite_name"])
        except Exception:
            context.add_expectation_suite(expectation_suite_name=spec["suite_name"])

        batch_request = RuntimeBatchRequest(
            datasource_name=datasource_name,
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name=spec["asset_name"],
            runtime_parameters={"query": spec["query"]},
            batch_identifiers={"run_id": "latest"},
        )

        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=spec["suite_name"],
        )
        spec["configure"](validator)
        validator.save_expectation_suite(discard_failed_expectations=False)
        validations.append(
            {
                "batch_request": batch_request,
                "expectation_suite_name": spec["suite_name"],
            }
        )

    checkpoint = SimpleCheckpoint(
        name="lakehouse_gold_checkpoint",
        data_context=context,
        validations=validations,
    )

    results = checkpoint.run()
    context.build_data_docs()

    output_path = CONTEXT_ROOT / "uncommitted" / "data_docs" / "local_site" / "index.html"
    print(f"success={results['success']}")
    print(f"data_docs={output_path}")


if __name__ == "__main__":
    main()

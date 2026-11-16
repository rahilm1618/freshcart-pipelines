"""
iris_project/run_pipeline.py
Standalone pipeline executor that mirrors Mage engine execution with full @test assertions.
"""

import os
import sys
import argparse
import pandas as pd
from iris_project.data_loaders.load_iris import load_data, test_output as test_load
from iris_project.transformers.check_no_missing import check_data, test_data_quality
from iris_project.transformers.prepare_data import transform as prep_data, test_output as test_prep
from iris_project.transformers.train_model import train as train_fn, test_accuracy
from iris_project.data_exporters.save_results import export_data
from iris_project.transformers.evaluate_model import evaluate


def execute_pipeline(max_depth=3, include_custom_block=True, include_eval_branch=True):
    print("=" * 60)
    print(f"Executing Mage Pipeline: iris_classifier (max_depth={max_depth})")
    print("=" * 60)

    # 1. load_iris
    print("\n[1/5] Executing Data Loader: load_iris...")
    df = load_data()
    test_load(df)
    print("  -> load_iris completed. @test test_output passed.")

    # 2. check_no_missing (Part C4 custom block)
    if include_custom_block:
        print("\n[2/5] Executing Transformer: check_no_missing (Part C4)...")
        df = check_data(df)
        test_data_quality(df)
        print("  -> check_no_missing completed. @test test_data_quality passed.")

    # 3. prepare_data
    print("\n[3/5] Executing Transformer: prepare_data...")
    df_prepared = prep_data(df)
    test_prep(df_prepared)
    print("  -> prepare_data completed. @test test_output passed.")

    # 4. train_model
    print(f"\n[4/5] Executing Transformer: train_model (max_depth={max_depth})...")
    predictions = train_fn(df_prepared, max_depth=max_depth)

    # Execute @test test_accuracy as a quality gate
    try:
        test_accuracy(predictions)
        print("  -> train_model @test test_accuracy PASSED (accuracy >= 0.80)")
    except AssertionError as e:
        print(f"  -> [CRITICAL] train_model @test FAILED: {e}")
        print("  -> Quality gate BLOCKED downstream execution (save_results and evaluate_model SKIPPED).")
        return {
            "max_depth": max_depth,
            "accuracy": round((predictions["predicted"] == predictions["target"]).mean(), 3),
            "test_passed": False,
            "csv_created": os.path.exists("iris_predictions.csv"),
        }

    # 5. Downstream Branch 1: save_results (Data Exporter)
    print("\n[5a/5] Executing Data Exporter: save_results...")
    export_data(predictions)
    csv_created = os.path.exists("iris_predictions.csv")
    print(f"  -> save_results completed. iris_predictions.csv exists: {csv_created}")

    # 5. Downstream Branch 2: evaluate_model (Transformer)
    if include_eval_branch:
        print("\n[5b/5] Executing Transformer: evaluate_model (Part C3 Evaluation Branch)...")
        metrics = evaluate(predictions)
        print("  -> evaluate_model completed successfully.")

    print("\n" + "=" * 60)
    print("PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
    print("=" * 60)

    return {
        "max_depth": max_depth,
        "accuracy": round((predictions["predicted"] == predictions["target"]).mean(), 3),
        "test_passed": True,
        "csv_created": csv_created,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-depth", type=int, default=3)
    args = parser.parse_args()
    execute_pipeline(max_depth=args.max_depth)


"""
iris_project/run_c2_experiments.py
Runs Part C2 test-gate experiments across max_depth = 1, 2, 3, 4, 5.
"""

import os
import sys
import pandas as pd
from iris_project.run_pipeline import execute_pipeline

OUTPUT_CSV = "iris_predictions.csv"


def run_experiments():
    results = []

    for depth in [1, 2, 3, 4, 5]:
        print("\n" + "#" * 65)
        print(f"# EXPERIMENT FOR max_depth = {depth}")
        print("#" * 65)

        # Before each run: delete iris_predictions.csv and confirm absence
        if os.path.exists(OUTPUT_CSV):
            os.remove(OUTPUT_CSV)
            print(f"[PRE-CHECK] Deleted existing {OUTPUT_CSV}")
        else:
            print(f"[PRE-CHECK] Confirmed {OUTPUT_CSV} is absent before run.")

        assert not os.path.exists(OUTPUT_CSV), f"Error: {OUTPUT_CSV} must not exist before run!"

        # Execute pipeline
        res = execute_pipeline(max_depth=depth, include_custom_block=False, include_eval_branch=False)

        # Confirm post-run CSV status
        csv_present = os.path.exists(OUTPUT_CSV)

        results.append({
            "max_depth": depth,
            "Test accuracy": res["accuracy"],
            "test_accuracy passed?": "YES" if res["test_passed"] else "NO (Failed)",
            "iris_predictions.csv created?": "YES" if csv_present else "NO (Gated/Skipped)",
        })

    summary_df = pd.DataFrame(results)
    print("\n" + "=" * 65)
    print("FINAL EXPERIMENT RESULTS (Part C2):")
    print("=" * 65)
    print(summary_df.to_string(index=False))

    summary_df.to_csv("c2_experiment_results.csv", index=False)
    return summary_df


if __name__ == "__main__":
    run_experiments()


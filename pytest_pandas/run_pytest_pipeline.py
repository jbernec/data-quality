import pytest
import sys

def run_tests():
    # Run Pytest and capture the exit code
    exit_code = pytest.main(["."])

    # Check the exit code to decide if the pipeline should stop
    if exit_code != 0:
        print("❌ Tests failed! Stopping the pipeline.")
        sys.exit(1)  # Exit with code 1 to indicate failure
    else:
        print("✅ All tests passed. Proceeding with the pipeline.")

if __name__ == "__main__":
    run_tests()

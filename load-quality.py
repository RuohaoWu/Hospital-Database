"""Run functions that updates the hospital database"""

import load_quality_functions as f
import sys


# Read Data
quality_data, current_pk, conn, cur = f.read_data(sys.argv[2])

# Insert/Update Data
rows_updated, new_hospital, rows_failed = f.insert_data(quality_data,
                                                        current_pk,
                                                        conn, cur,
                                                        sys.argv[1])

# Display Insert/Update Metrics
f.display_results(rows_updated, new_hospital, rows_failed)

"""Run functions that updates the hospital database"""

import load_hhs_functions as f
import sys

# Read Data
data, d, conn, cur = f.read_data(sys.argv[1])

# Insert/Update Data
rows_updated, new_hospital, rows_failed = f.insert_data(data, d, conn, cur)

# Display Insert/Update Metrics
f.display_results(rows_updated, new_hospital, rows_failed)
import sys

from pyarrow.json import read_json
from pyarrow.parquet import write_table

input = sys.stdin.buffer

table = read_json(input)
write_table(table, 'kimetsu_py.parquet')
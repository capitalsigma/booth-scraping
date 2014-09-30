import traceback
import sys

from multiprocessing import Pool
from amazon import main

OUTFILE_FMT = "big-run/{}-to-{}.csv"

def wrapped_run(start_index):
    try:
        do_run(start_index)
    except Exception as e:
        print("SOMETHING WENT WRONG: {}".format(e))
        traceback.print_exc()

def do_run(start_index):
    start = start_index * 100
    end = (start_index + 1) * 100
    outfile = OUTFILE_FMT.format(start, end)
    print("Running for {}, {}, {}".format(
        start, end, outfile))
    main(outfile, start, end)
    # try:
    #     main(outfile, start, end)
    # except Exception as e:
    #     # (type, value, traceback) = sys.exc_info()
    #     print("SOMETHING WENT WRONG!: {}".format(e))
    #     with open(OUTFILE_FMT.format(start, "ERROR"), "w") as f:
    #         traceback.print_last(file=f)

# wrapped_run(1)
with Pool(12) as pool:
    pool.map(wrapped_run, range(1, 11000))
# for run in range(0, 12):
#     wrapped_run(run)

# for start_index in range(1, 12):
#     try:
#         do_run(start_index)
#     except Exception as e:
#         print("SOMETHING WENT WRONG: {}".format(e))

from amazon import main

OUTFILE_FMT = "big-run/{}-to-{}.csv"

for start_index in range(1, 12):
    start = start_index * 100000
    end = (start_index + 1) * 100000
    outfile = OUTFILE_FMT.format(start, end)
    try:
        main(outfile, start, end)
    except Exception as e:
        print("SOMETHING WENT WRONG!: {}".format(e))
        with open(OUTFILE_FMT.format(start, "ERROR")) as f:
            f.write("ERROR: {}".format(e))

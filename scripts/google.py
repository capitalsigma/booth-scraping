import requests
import argparse
import csv
import re
import time

from bs4 import BeautifulSoup

PROXY_IP = "localhost"
PROXY_PORT = "8118"

def get_query(query_text):
    BASE_URL = "http://www.google.com/search"
    USER_AGENT = {'User-agent': 'Mozilla/5.0'}
    PROXIES = {
        "http": "http://{}:{}".format(PROXY_IP, PROXY_PORT)
    }

    return requests.get(BASE_URL,
                        params={"q":query_text,
                                "client":"ubuntu",
                                "channel":"fs",
                                "ie":"utf-8",
                                "oe":"utf-8"},
                        headers=USER_AGENT,
                        proxies=PROXIES)

def get_result(search_term, wait=5):
    RESULT_ID = "resultStats"
    NUMERIC_REGEX = "[0-9,]+"

    time.sleep(wait)

    query = get_query(search_term)

    soup = BeautifulSoup(query.text)

    # print("Got soup: {}".format(soup))

    result_div = soup.find(id=RESULT_ID)

    # print("Got tag: {}".format(result_div))

    result_str = result_div.text

    # print("Got result string: {}".format(result_str))

    return int(re.search(
        NUMERIC_REGEX, result_str).group(0).replace(",", ""))

def main(infile_path, outfile_path):
    DEFAULT_OUT = "results.csv"
    result = []
    with open(infile_path, newline='', encoding="mac_roman") as f:
        for line in csv.reader(f):
            this_row = []
            for entry in line:
                try:
                    this_row.extend([entry, get_result(entry)])

                except Exception as e:
                    print("Something went wrong! Got error: {}".format(e))

                    this_row.extend([entry, "ERROR"])

                print("Built row: {}".format(this_row))

                result.append(this_row)

        print("Built result: {}".format(result))

    with open(outfile_path or DEFAULT_OUT, "w") as out_f:
        writer = csv.writer(out_f)
        writer.writerows(result)



if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        description="Scrape the number of google search results")

    arg_parser.add_argument(
        "infile_path",
        metavar="input.csv",
        nargs=1)

    arg_parser.add_argument(
        "outfile_path",
        metavar="output.csv",
        nargs="?")

    args = arg_parser.parse_args()

    main(args.infile_path[0], args.outfile_path)

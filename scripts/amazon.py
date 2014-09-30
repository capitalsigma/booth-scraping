import requests
import argparse
import csv
import re
import collections
import math
import logging

logging.basicConfig(level=logging.ERROR)
LOG = logging.getLogger(__name__)

from bs4 import BeautifulSoup

# PROXY_IP = "localhost"
# PROXY_PORT = "8118"

class BookNotFoundError(Exception):
    pass

class QuoteNotFoundError(Exception):
    pass

BookData = collections.namedtuple("BookData",
                                  ["overall_rank",
                                   "individual_rank",
                                   "info",
                                   "rating",
                                   "review_count",
                                   "public_notes"])
QuoteData = collections.namedtuple("QuoteData",
                                   ["number",
                                    "text",
                                    "highlighted_count",
                                    "title",
                                    "author"])

HEADERS = ["Number", "Text", "Highlighted Count", "Title", "Author",
           "Rating", "Overall Rank", "Review Count"]

class NullEntry:
    def __bool__(self):
        return False

    def __getattr__(self, attr):
        return ""

class Scraper:
    BASE_URL_FMT = "https://kindle.amazon.com/most_popular/highlights_all_time/{}"
    KINDLE_URL_FMT = "https://kindle.amazon.com{}"
    USER_AGENT = {'User-agent': 'Mozilla/5.0'}

    OVERALL_REGEX = r"#([0-9,]+) (Paid|Free) in Kindle Store"
    LINE_REGEX = r"\s* #([0-9,]+) \n .*?>(.*)"
    WS_REGEX = r"\s+"
    NOT_DIGITS_REGEX = "[^0-9]"
    DIGITS_REGEX = r"(\d+)"

    def __init__(self):
        self._cached_book_urls = {}
        self._books = {}

        self._overall_re = re.compile(self.OVERALL_REGEX)
        self._line_re = re.compile(self.LINE_REGEX)
        self._ws_re = re.compile(self.WS_REGEX)
        self._not_digits_re = re.compile(self.NOT_DIGITS_REGEX)
        self._digits_re = re.compile(self.DIGITS_REGEX)


    def _make_request(self, url):
        while True:
            try:
                return requests.get(url,
                                    headers=self.USER_AGENT)
            except requests.ConnectionError as e:
                LOG.error("Got connection error: {}. Retrying".format(e))


    def _make_book_request(self, book_url):
        ret = self._make_request(book_url)

        LOG.info("got request status code: {}".format(ret.status_code))

        if ret.status_code != 200:
            raise BookNotFoundError("URL: {}, status code: {}".format(book_url, ret.status_code))

        return ret

    def _make_kindle_request(self, partial_url):
        return self._make_book_request(self.KINDLE_URL_FMT.format(partial_url))

    def _get_most_highlighted_from(self, first):
        return self._make_request(self.BASE_URL_FMT.format(first))

    def _get_overall_rank(self, tag_text):
        result = self._overall_re.search(tag_text)

        try:
            # #LOG.debug("got overall ranks group 0: {}, 1: {}, 2: {}".format(
            #     *[result.group(i) for i in range(3)]))
            return "#{} {}".format(result.group(1), result.group(2))

        except AttributeError:
            return None


    def _get_individual_ranks(self, tag_text):
        new_tag_text = tag_text
        search_result = self._line_re.search(new_tag_text)
        to_ret = {}

        while search_result:
            number = search_result.group(1)
            section = search_result.group(2).strip()

            to_ret[section] = number

            new_tag_text = new_tag_text.replace(search_result.group(0), "")

            search_result = self._line_re.search(new_tag_text)

            #LOG.debug("got search_result: {}".format(search_result))

        return to_ret

    def _strip_ws(self, text_to_strip):
        return self._ws_re.sub(" ", text_to_strip).strip()

    def _get_product_info(self, content_div):
        to_ret = {}

        for tag in content_div.findAll("li"):
            if tag.has_attr('id') and tag.attrs['id'] == 'SalesRank':
                break
            else:
                line_text = self._strip_ws(tag.text)

                prop, value = line_text.split(":", maxsplit=1)
                to_ret[prop] = value
        return to_ret

    def _get_rating(self, rating_tag):
        if rating_tag:
            return rating_tag.text.split("out", 1)[0].strip()
        else:
            return ""

    def _get_review_count(self, count_tag):
        LOG.debug("Grabbing for count tag: {}={}".format(
            count_tag, count_tag and count_tag.text))
        if count_tag and count_tag.text:

            # we do this because a single review parses to 'see the
            # review' rather than 'see all \d+ reviews'
            return int(self._not_digits_re.sub("", count_tag.text) or 1)
        else:
            return ""

    def _process_book_page(self, page_soup):
        LOG.debug("processing page: {}".format(page_soup.find("title")))

        rankings_tag = page_soup.find(id="SalesRank")

        try:
            overall_rank = self._get_overall_rank(rankings_tag.text)
        except AttributeError:
            raise BookNotFoundError

        LOG.debug("got overall_rank: {}".format(overall_rank))

        individual_ranks = self._get_individual_ranks(rankings_tag.text)

        LOG.debug("got individual_ranks: {}".format(individual_ranks))

        prod_details = page_soup.find("h2", text="Product Details")

        LOG.debug("got prod details: {}".format(prod_details))

        product_info = self._get_product_info(
            prod_details.next_sibling.next_sibling)

        LOG.debug("got prod_info: {}".format(product_info))

        rating = self._get_rating(
            page_soup.find(class_="gry txtnormal acrRating"))

        LOG.debug("got rating: {}".format(rating))

        review_count = self._get_review_count(
            page_soup.find(id="revSAR"))

        return BookData(overall_rank,
                        individual_ranks,
                        product_info,
                        rating,
                        review_count,
                        "")

    def _get_num(self, number_tag):
        return int(number_tag.text.strip(" \n."))

    def _get_highlighted_count(self, highlight_tag):
        return re.search(r"(\d+)", highlight_tag.text).group(0)

    def _get_author(self, author_tag):
        return author_tag.text.split("by")[-1].strip()

    def _process_quote_tag(self, quote_tag):
        num = self._get_num(quote_tag.find(class_="number"))
        text = quote_tag.find(class_="highlight").text
        highlighted_by = self._get_highlighted_count(
            quote_tag.find(class_="highlightedBy"))
        title = quote_tag.find(class_="title").text
        author = self._get_author(quote_tag.find(class_="author"))

        return QuoteData(num, text, highlighted_by, title, author)

    def _get_url(self, see_link_tag):
        return see_link_tag.find("a").attrs['href']

    def _process_kindle_book_page(self, page_soup):
        LOG.debug("Processing Kindle page.")

        count_span = page_soup.find(class_="count")
        LOG.debug("Got count_span: {}".format(count_span))
        review_count = self._not_digits_re.sub("", count_span.text)

        stars_tag = count_span.parent.find("img")
        rating = re.sub("[^0-9.]", "", stars_tag.attrs['alt'])

        LOG.debug("stars tag: {}".format(stars_tag))
        LOG.debug("rating: {}".format(rating))

        public_notes = page_soup.find(id="subscriberLink").text

        ret = BookData("",
                       {},
                       {},
                       rating,
                       review_count,
                       public_notes)

        LOG.debug("Got partial book data: {}".format(ret))

        return ret


    def _get_new_book_page(self, url, kindle_book_url):
        to_ret = BookData(None, None, None, None, None, None)

        # they seem to get intermittent 500 errors, so we retry
        for _ in range(5):
            try:
                kindle_soup = BeautifulSoup(
                    self._make_kindle_request(kindle_book_url).text)
                to_ret = self._process_kindle_book_page(kindle_soup)
                break
            except (ConnectionError, BookNotFoundError):
                pass

        try:
            book_soup = BeautifulSoup(self._make_book_request(url).text)
            book_tup = self._process_book_page(book_soup)
            return self._merge_tuples(to_ret, book_tup, BookData)
        except (ConnectionError, BookNotFoundError):
            return to_ret

    # left takes precedence
    def _merge_tuples(self, lefts, rights, constructor):
        LOG.debug("merging left: {}, right: {}".format(lefts, rights))

        return constructor(*[left or right for
                             left, right in zip(lefts, rights)])

    def _process_book(self, quote_tag):
        try:
            quote_data = self._process_quote_tag(quote_tag)
        except AttributeError:
            raise QuoteNotFoundError

        LOG.debug("built quote_data: {}".format(quote_data))

        url = self._get_url(quote_tag.find(class_="seeLink"))
        kindle_book_url = self._get_url(quote_tag.find(class_="title"))

        try:
            book_data = self._cached_book_urls[url]
        except KeyError:
            book_data = self._get_new_book_page(url, kindle_book_url)
            self._cached_book_urls[url] = book_data

        LOG.debug("built book_data: {}".format(book_data))

        self._books[quote_data] = book_data

        return quote_data.number

    def _round_up_to_next_chunk(self, value):
        return math.ceil(value/25)*25 + 1

    def _process_highlights_page(self, highlights_page):
        last_number = -1
        for row in highlights_page.findAll(class_="listRow"):
            try:
                last_number = self._process_book(row)
            except QuoteNotFoundError:
                last_number += 1

            LOG.debug("Parsed for #{}".format(last_number))

        return last_number

    def scrape(self, start_num, end_num):
        current_num = start_num
        prev_num = -1

        while current_num < end_num:
            LOG.info("Parsing number {}".format(current_num))

            highlighted_soup = BeautifulSoup(
                self._get_most_highlighted_from(current_num).text)

            prev_num = current_num
            current_num = 1 + self._process_highlights_page(highlighted_soup)

            if current_num <= prev_num:
                # we're stuck, break out
                current_num = self._round_up_to_next_chunk(prev_num)

            LOG.debug("current: {}".format(current_num))
            LOG.debug("prev: {}".format(prev_num))

        LOG.debug(self._books)

        return self._books


def get_unique_rankings(all_book_data):
    all_rankings = []
    for book in [book for book in all_book_data if book]:
        all_rankings.extend(book.individual_rank.keys())

    return list(set(all_rankings))

def get_unique_infos(all_book_data):
    all_infos = []
    for book in [book for book in all_book_data if book]:
        all_infos.extend(book.info.keys())

    return list(set(all_infos))

def build_default_header_section(quote_data, book_data):
    return [
        quote_data.number,
        quote_data.text,
        quote_data.highlighted_count,
        quote_data.title,
        quote_data.author,
        book_data.rating,
        book_data.overall_rank,
        book_data.review_count
    ]

def build_info_header_section(book_data, unique_infos):
    if book_data:
        return [book_data.info.get(info, "") for info in unique_infos]
    else:
        return ["" for info in unique_infos]

def build_rank_header_section(book_data, unique_rankings):
    if book_data:
        return [book_data.individual_rank.get(info, "")
                for info in unique_rankings]
    else:
        return ["" for info in unique_rankings]

def write_books_to_csv(books, out_file):
    unique_rankings = get_unique_rankings(books.values())
    unique_infos = get_unique_infos(books.values())

    headers = HEADERS + unique_rankings + unique_infos

    writer = csv.writer(out_file)

    writer.writerow(headers)

    for quote_data, book_data in sorted(
            books.items(), key=lambda x: x[0].number):

        default_header_section = build_default_header_section(
            quote_data, book_data)

        LOG.debug("got default header section: {}".format(
            default_header_section))

        info_header_section = build_info_header_section(book_data,
                                                        unique_infos)

        LOG.debug("got info header section: {}".format(
            info_header_section))

        ranking_header_section = build_rank_header_section(book_data,
                                                           unique_rankings)

        LOG.debug("got ranking header section: {}".format(ranking_header_section))

        writer.writerow(
            default_header_section +
            ranking_header_section +
            info_header_section)



def main(outfile, start, end):
    scraper = Scraper()
    books = scraper.scrape(start, end)

    with open(outfile, "w") as f:
        write_books_to_csv(books, f)



if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        description="Scrape Amazon Kindle data")

    arg_parser.add_argument(
        "outfile_path",
        metavar="output.csv",
        nargs=1)

    arg_parser.add_argument(
        "start",
        metavar="startN",
        nargs=1)

    arg_parser.add_argument(
        "end",
        metavar="endN",
        nargs=1)

    args = arg_parser.parse_args()

    main(args.outfile_path[0], int(args.start[0]), int(args.end[0]))

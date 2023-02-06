import io
import logging
import re
from urllib.parse import urlparse
import lxml.etree
from lxml.html import iterlinks, make_links_absolute

logger = logging.getLogger(__name__)


class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus

    def get_next_token(self, file):
        token = ''

        while True:
            c = file.read(1)
            if len(c) == 0:
                if len(token) > 0:
                    return token
                else:
                    return None

            if c.isalnum() and c.isascii():
                token = token + c.lower()
            elif len(token) > 0:
                return token

    def tokenize_string(self, value):
        tokens = []
        with io.StringIO(value) as file:
            while True:
                token = self.get_next_token(file)
                if token != None:
                    tokens.append(token)
                else:
                    return tokens

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        stopwords = "a about above after again against all am an and any are aren't as at be because been before being below between both but by can't cannot \
         could couldn't did didn't do does doesn't doing don't down during each few for from further had hadn't has hasn't have haven't having he \
         he'd he'll he's her here here's hers herself him himself his how how's i i'd i'll i'm i've if in into is isn't it it's its itself let's \
         me more most mustn't my myself no nor not of off on once only or other ought our ours ourselves out over own same shan't she \
         she'd she'll she's should shouldn't so some such than that that's the their theirs them themselves then there \
         there's these they they'd they'll they're they've this those through to too under until up very was wasn't we we'd we'll we're \
         we've were weren't what what's when when's where where's which while who who's whom why why's with won't would wouldn't you you'd \
         you'll you'e you've you yours yourself yourselves".split(' ')

        subdomains = {}
        mostoutlinks = 0
        mostoutlinksURL = ''
        downloadedURLS = []
        trapURLS = []
        mostwordcount = 0
        longestpage = ''
        commonwords = {}
        with open('analytics.txt', 'w') as textfile:

            while self.frontier.has_next_url():
                url = self.frontier.get_next_url()
                logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched,
                            len(self.frontier))
                url_data = self.corpus.fetch_url(url)

                parsed = urlparse(url_data['url'])
                subdomain = parsed.netloc

                regex = re.compile(r'<[^>]+>|\&[^;]+;|\\[nrt]|\\|<script.*?/script>')
                ctext = regex.sub('', str(url_data['content']))
                ctext = re.sub(' +', ' ', ctext)
                ctextList = self.tokenize_string(ctext)

                for word in ctextList:
                    if word not in stopwords:
                        if word in commonwords:
                            commonwords[word] = commonwords[word] + 1
                        else:
                            commonwords[word] = 1

                if len(ctextList) > mostwordcount:
                    mostwordcount = len(ctextList)
                    longestpage = url_data['url']

                if subdomain in subdomains:
                    subdomains[subdomain] = subdomains[subdomain] + 1
                else:
                    subdomains[subdomain] = 1

                next_links = self.extract_next_links(url_data)

                if len(next_links) > mostoutlinks:
                    mostoutlinks = len(next_links)
                    mostoutlinksURL = url_data['url']

                for next_link in next_links:
                    if self.is_valid(next_link):
                        if self.corpus.get_file_name(next_link) is not None:
                            self.frontier.add_url(next_link)
                        downloadedURLS.append(next_link)
                    else:
                        trapURLS.append(next_link)

            print("Subdomains: " + str(subdomains), file=textfile)
            print(" ")
            print("Page with Most Valid OutLinks: " + mostoutlinksURL + " Length: " + str(mostoutlinks), file=textfile)
            print(" ")
            print("Downloaded URLs:", file=textfile)
            for url in downloadedURLS:
                print('    ' + url, file=textfile)
            print(" ")
            print("Trap URLS:", file=textfile)
            for url in trapURLS:
                print('    ' + url, file=textfile)
            print(" ")
            print("Longest Page by Words: " + longestpage + " Word Count: " + str(mostwordcount), file=textfile)
            print(" ")
            print("Most Common Words: ", file=textfile)
            sortedcommonwords = sorted(commonwords.items(), key=lambda keyval: (-keyval[1], keyval[0]))
            wordcount = len(sortedcommonwords)
            if wordcount > 50:
                wordcount = 50
            for i in range(0, wordcount):
                print('    ' + sortedcommonwords[i][0], file=textfile)

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        outputLinks = []

        if url_data["content_type"] is not None and ("application" in url_data["content_type"] or "xml"
                                                     in url_data["content_type"] or "calendar" in url_data[
                                                         "content_type"]):
            return outputLinks

        if 1000000 > url_data["size"] > 0 and url_data["http_code"] != 400 and 499 != url_data["http_code"]:
            try:
                absolute_http = make_links_absolute(url_data["content"], url_data["url"])
                for _, _, url, _ in iterlinks(absolute_http):
                    outputLinks.append(url)
            except lxml.etree.ParserError:
                return outputLinks
            except ValueError:
                return outputLinks
        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        # print(url)

        if len(parsed.query) > 15 or len(parsed.geturl()) > 55:
            return False
        if "share=" in parsed.query or "action=download" in parsed.query:
            return False
        if parsed.scheme not in set(["http", "https"]):
            return False
        if len(parsed.path.split('/')) > 5:
            return False
        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf|sql|r|sas|wvx|jpg|webm|py|db|tsv" \
                                    + "|klg|ply|java|war|exr|mpg|DS_Store|hdf5|seq|bam|npz|bw)$", parsed.path.lower())
        # ADDED: sql, sas, r
        except TypeError:
            print("TypeError for ", parsed)
            return False

import logging

import lxml
from collections import defaultdict

from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager, Link, UrlResponse
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import etree,html
import re, os

from os.path import exists
from time import time

try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set()
             if not os.path.exists("successful_urls.txt") else
             set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""]))
MAX_LINKS_TO_DOWNLOAD = 3000



@Producer(ProducedLink, Link)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):
    def __init__(self, frame):
        self.starttime = time()
        
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "70116153_58042643_57615347"
        
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR S17 UnderGrad 70116153, 58042643, 57615347"

        self.frame = frame
        assert (self.UserAgentString != None)
        assert (self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print l.full_url
        self.frame.add(l)
        readPrevAnalytics()

    def update(self):
        for g in self.frame.get_new(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url):
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        analytics() ## Record analytics of the crawler to analytics.txt
        print "downloaded ", len(url_count), " in ", time() - self.starttime, " seconds."
        pass

def save_count(urls):
    global url_count
    urls = set(urls).difference(url_count)
    url_count.update(urls)
    if len(urls):
        with open("successful_urls.txt", "a") as surls:
            surls.write(("\n".join(urls) + "\n").encode("utf-8"))


def process_url_group(group, useragentstr):
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    save_count(successfull_urls)
    return extract_next_links(rawDatas), rawDatas


## A | N | A | L | Y | T | I | C | S #######################################################
invalid_links = 0
PageWithMostOutLinks = None
subdomains = defaultdict(int)

def readPrevAnalytics():
    
    if not exists("analytics.txt"):
        return

    file = open("analytics.txt", "r")

    global subdomains
    global PageWithMostOutLinks
    global invalid_links
    for line in file.readlines():
        line = line.split(" ")
        
        if line[0] == "Subdomain":
            subdomains[line[1]] += int(line[2])
        
        elif line[0] == "Page":
            PageWithMostOutLinks = (line[1], int(line[2]))
        
        elif line[0] == "Invalidlinks":
            invalid_links += int(line[1])

    file.close()

def analytics():
    file = open("analytics.txt", "w")
    
    global subdomains
    file.write("S | U | B | D | O | M | A | I | N | S \n")
    for name, visits in subdomains.items():
        file.write( "Subdomain " + name + " " + str(visits) + "\n")

    global PageWithMostOutLinks
    file.write("\nM | O | S | T |  | O | U | T | L | I | N | K | S \n")
    if PageWithMostOutLinks is not None:
        file.write("Page "  + PageWithMostOutLinks[0] \
            + " " + str(PageWithMostOutLinks[1]) +"\n")
    else:
        file.write("No page with most out Links\n")

    global invalid_links
    file.write("\nI | N | V | A | L | I | D |  | L | I | N | K | S \n")
    file.write("Invalidlinks " + str(invalid_links) + "\n")

    file.close()


#######################################################################################
'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''

def extract_next_links(rawDatas):
    '''
    rawDatas is a list of objs -> [raw_content_obj1, raw_content_obj2, ....]
    Each obj is of type UrlResponse  declared at L28-42 datamodel/search/datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded.
    The frontier takes care of that.

    Suggested library: lxml
    '''

    outputLinks = list()

    for raw_content in rawDatas:
        assert isinstance(raw_content, UrlResponse)
        if not raw_content.bad_url and raw_content.content is not None:
            try: 
                html_string = lxml.html.fromstring(raw_content.content)

                if not raw_content.is_redirected:
                    html_string.make_links_absolute(raw_content.url)
                else:
                    html_string.make_links_absolute(raw_content.final_url)

                for element, attribute, link, position in lxml.html.iterlinks(html_string):
                    print link
                    outputLinks.append(link)
                    raw_content.out_links.add(link)
                    
                global PageWithMostOutLinks
                if PageWithMostOutLinks is not None:
                    if len(raw_content.out_links) > PageWithMostOutLinks[1]:
                        PageWithMostOutLinks = (raw_content.url, len(raw_content.out_links))
                else:
                    PageWithMostOutLinks = (raw_content.url, len(raw_content.out_links))
            
            except:
                ## if url parsing the url fails, mark it as a bad url 
                raw_content.bad_url = True

    
    print "------------------------------------------"
    return outputLinks

def _validPath(parsedUrlPath):
    ''' Checks to see if there are duplicate directories in the path. '''
    
    pathDirectories = parsedUrlPath[1:].split("/")

    # Depth Checking, path components of at most 10 will be considered valid
    if len(pathDirectories) > 10:
        return False
    
    for directory in pathDirectories[:-1]:
        if "." in directory:
            return False

    return (len(pathDirectories) == len(set(pathDirectories)))


def _noQuery(parsedUrlQuery, parsedUrlFrags):
    ## To avoid being trapped
    return len(parsedUrlQuery) == 0 and len(parsedUrlFrags) == 0

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.
    
    This is a great place to filter out crawler traps.
    '''

    global invalid_links
    #badLinks = open("badlinks.txt", "a")
    parsed = urlparse(url)
    
    try: 
        validity = parsed.scheme in set(["http", "https"]) \
                    and ".ics.uci.edu" in parsed.hostname \
                    and _validPath(parsed.path) \
                    and _noQuery(parsed.query, parsed.fragment) \
                    and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" \
                                    + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data" \
                                    + "|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

        print url + "\n"
        if validity == False:
            invalid_links += 1
            #badLinks.write(url + "\n")
            #badLinks.close()
        else:
            # Increment the number of times the subdomain is visited
            global subdomains
            subdomain = parsed.hostname.split('.')[0]
            subdomains[subdomain] += 1

        #badLinks.close()
        return validity

    except TypeError:
        print ("TypeError for ", parsed)
        invalid_links += 1
        #badLinks.write(url + "\n")
        #badLinks.close()
        return False

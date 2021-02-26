import logging
import os
from urllib.parse import urlparse
import re

def is_valid(url):
    if url is None:
        return False

    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or parsed.netloc.find('://') != -1:
            return False
        # special rules for trash pages:
        if parsed.path.find('wp-json') != -1: # example: https://ngs.ics.uci.edu/wp-json/wp/v2/posts/1234
            return False
        # example: https://wics.ics.uci.edu/events/category/wics-meeting-2/2016-09-30/
        if parsed.query.find('ical') != -1 or (parsed.netloc == 'wics.ics.uci.edu' and parsed.path.find('event') != -1):
            return False
        if parsed.path.find('public_data') != -1:
            return False
            
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1|xml|json"
            + r"|thmx|mso|arff|rtf|jar|csv|embed|ppsx"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise

def get_logger(name, filename=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not os.path.exists("Logs"):
        os.makedirs("Logs")
    fh = logging.FileHandler(f"Logs/{filename if filename else name}.log")
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
       "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger
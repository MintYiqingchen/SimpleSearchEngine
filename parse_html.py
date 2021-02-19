import json
from bs4 import BeautifulSoup
import re
import string
from collections import defaultdict
from gensim.parsing.porter import PorterStemmer
from string import punctuation as p
from urllib.parse import urlparse

# file_dict = {}
# with open(path, 'rt') as f:
#     file_dict = json.load(f)

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

class Parser:
    def reset(self, content):
        self.content = content
        self.soup = BeautifulSoup(self.content, 'html.parser')
        self.base_text_dict = defaultdict(list)  # k:word, v:occurrencelist}
        self.anchor_dict = defaultdict(list)  # {k: word, v:urllist}

    def parse(self, content):
        self.reset(content)

        for tag in self.soup.find_all(['style', 'script']):
            tag.decompose()

        # filter low info pages or pure data page
        if not self.soup.html:
            return {}, {}

        s_title = self._get_title()
        s_heading = self._get_heading()
        s_bold = self._get_bold()
        s_italic = self._get_italic()
        text = self._get_text()
        stemmed_text = self._stem_string(text, p)
        self._get_base_text_dict(stemmed_text)
        word_pos_dict = self._get_word_pos_dict(stemmed_text)
        self._update_format_code(stemmed_text, word_pos_dict, s_title, s_heading, s_bold, s_italic)

        aTags, aUrl = self._get_anchor()

        self._make_anchor_dict(aTags, aUrl)

        return self.base_text_dict, self.anchor_dict

    def _make_anchor_dict(self, aTags, aUrl):
        for i in range(len(aTags)):
            for w in aTags[i].strip().split(" "):
                self.anchor_dict[w].append(aUrl[i])


    # title
    def _get_title(self):
        tTags = []
        titles = self.soup.find_all('title')
        
        for t in titles:
            title = t.text
            tTags.append(self._stem_string(title, p))
        return tTags

    def _get_heading(self):
        hTags = []
        for headlings in self.soup.find_all(re.compile('^h[1-6]$')):
            heading = headlings.text.strip()
            hTags.append(self._stem_string(heading, p))
        return hTags

    # bold word
    def _get_bold(self):
        bTags = []
        for i in self.soup.findAll('b'):
            bold = i.text.strip()
            bTags.append(self._stem_string(bold, p))
        return bTags

    # italic word
    def _get_italic(self):
        iTags = []
        for i in self.soup.findAll('i'):
            italic = i.text.strip()
            iTags.append(self._stem_string(italic, p))
        return iTags

    # anchor word
    def _get_anchor(self):
        aTags = []
        aUrl = []
        for i in self.soup.find_all(href=is_valid, rel=lambda x: (x is None or x != 'nofollow')):
            aTags.append(self._stem_string(i.text.strip(), p))
            aUrl.append(i['href'])  # there are non-html content, like email address
        return aTags, aUrl

    # all text
    def _get_text(self):
        return self.soup.text

    def _isEnglish(self, s):
        try:
            s.encode(encoding='utf-8').decode('ascii')
        except UnicodeDecodeError:
            return False
        else:
            return True

    # input: text
    # return: cleaned base string "all word context" (NOTE: '\n'etc should be removed)

    def _stem_string(self, text, p):
        punctuation = re.compile('[{}]+'.format(re.escape(p)))
        clean_string = ""
        for line in text.splitlines():
            for word in line.strip().lower().split(" "):
                word = word.strip(string.punctuation)
                new_word = ''
                for c in word:
                    if self._isEnglish(c):
                        new_word = new_word + c
                new_word = punctuation.sub('', new_word)  # added
                if new_word != "":
                    clean_string = clean_string + " " + new_word
        p = PorterStemmer()

        return p.stem_sentence(clean_string)

    # input: cleaned stemed text as one string
    # output: base text dict {"word":[[pos1, 0], [pos2, 0],...]}

    def _get_base_text_dict(self, stem_text):
        for i in range(len(stem_text.split(" "))):
            word = stem_text.split(" ")[i]
            self.base_text_dict[word].append([i, 0])

    # input: cleaned stemed text as one string
    # output: pos dict {char_pos_index: word_pos_index}

    def _get_word_pos_dict(self, stem_text):
        word_pos_dict = {0: 0}
        # word_len = len(stem_text.split(" ")[0])
        char_index = 0
        stem_list = stem_text.split()
        for i in range(1, len(stem_list)):
            char_index += len(stem_list[i - 1]) + 1

            word_pos_dict[char_index] = i

        return word_pos_dict

    def _format_code_helper(self, clean_string, word_pos_dict, s, code):
        # print(word_pos_dict)
        # print(s)
        for t in s:
            if t != "":
                i = clean_string.find(t)
                try:
                    start = word_pos_dict[i]
                except(KeyError):
                    continue
                end = start + len(t.split(" "))
                for w in t.strip().split(" "):
                    pos_list = self.base_text_dict[w]
                    # print(pos_list)
                    for i in range(len(pos_list)):
                        if start <= pos_list[i][0] < end:
                            self.base_text_dict[w][i][1] = code

    # code priority
    # title: 4 > headings: 3 > bold: 2 > italic: 1 > plain: 0
    def _update_format_code(self, clean_string, word_pos_dict,
                            s_title, s_head, s_bold, s_italic):
        self._format_code_helper(clean_string, word_pos_dict, s_italic, 1)
        self._format_code_helper(clean_string, word_pos_dict, s_bold, 2)
        self._format_code_helper(clean_string, word_pos_dict, s_head, 3)
        self._format_code_helper(clean_string, word_pos_dict, s_title, 4)








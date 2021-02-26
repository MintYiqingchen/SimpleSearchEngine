import json
from bs4 import BeautifulSoup
import re
import string
from collections import defaultdict
from gensim.parsing.porter import PorterStemmer
from string import punctuation as p
from utils import is_valid
from serialize import PLNode

# file_dict = {}
# with open(path, 'rt') as f:
#     file_dict = json.load(f)

class Parser:
    def reset(self, content):
        self.content = content
        self.soup = BeautifulSoup(self.content, 'html.parser')
        self.base_text_dict = {}  # {k:word, v:PLNode}
        self.anchor_dict = defaultdict(set)  # {k: word, v:urlset}

    def parse(self, content):
        self.reset(content)

        for tag in self.soup.find_all(['style', 'script']):
            tag.decompose()

        # filter low info pages or pure data page
        if not self.soup.html:
            return {}, {}, 0

        s_title = self._get_title()
        s_heading = self._get_heading()
        s_bold = self._get_bold()
        s_italic = self._get_italic()
        text = self._get_text(self.soup)
        stemmed_text = self._stem_string(text, p)
        self._get_base_text_dict(stemmed_text)
        # word_pos_dict = self._get_word_pos_dict(stemmed_text)
        self._update_format_code(stemmed_text, s_title, s_heading, s_bold, s_italic)

        aTags, aUrl = self.get_anchor()

        self._make_anchor_dict(aTags, aUrl)

        # calculate weighted tf considering font
        weight_sum = self.numWords
        for k, v in self.font_counter.items():
            weight_sum += k * v
        for k, v in self.base_text_dict.items():
            v.tf /= weight_sum

        return self.base_text_dict, self.anchor_dict, self.numWords

    def _make_anchor_dict(self, aTags, aUrl):
        for i in range(len(aTags)):
            for w in aTags[i].strip().split(" "):
                self.anchor_dict[w].add(aUrl[i])


    # title
    def _get_title(self):
        tTags = []
        titles = self.soup.find_all('title')
        
        for t in titles:
            title = self._get_text(t)
            tTags.append(self._stem_string(title, p))
        return tTags

    def _get_heading(self):
        hTags = []
        for headlings in self.soup.find_all(re.compile('^h[1-6]$')):
            heading = self._get_text(headlings)
            hTags.append(self._stem_string(heading, p))
        return hTags

    # bold word
    def _get_bold(self):
        bTags = []
        for i in self.soup.findAll('b'):
            bold = self._get_text(i)
            bTags.append(self._stem_string(bold, p))
        return bTags

    # italic word
    def _get_italic(self):
        iTags = []
        for i in self.soup.findAll('i'):
            italic = self._get_text(i)
            iTags.append(self._stem_string(italic, p))
        return iTags

    # anchor word
    def get_anchor(self):
        aTags = []
        aUrl = []
        for i in self.soup.find_all(href=is_valid, rel=lambda x: (x is None or x != 'nofollow')):
            aTags.append(self._stem_string(self._get_text(i), p))
            aUrl.append(i['href'])  # there are non-html content, like email address
        return aTags, aUrl

    # all text
    def _get_text(self, elem):
        return ' '.join(elem.stripped_strings)

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
        delim = re.compile(r'[\W_]+', re.ASCII)
        clean_string = delim.sub(' ', text.strip().lower())
        p = PorterStemmer()

        return p.stem_sentence(clean_string)

    # input: cleaned stemed text as one string
    # output: base text dict {"word":PLNode}

    def _get_base_text_dict(self, stem_text):
        words = stem_text.split()
        self.numWords = len(words)
        for i in range(self.numWords):
            word = words[i]
            if word in self.base_text_dict:
                self.base_text_dict[word].occurrences.append(i)
                self.base_text_dict[word].tf += 1
            else:
                self.base_text_dict[word] = PLNode(-1, 1, [i])

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

    def _format_code_helper(self, clean_string, s, code):
        # print(word_pos_dict)
        # print(s)
        for t in s:
            if t != "":
                words = t.strip().split()
                self.font_counter[code] += len(words)
                for w in words:
                    self.base_text_dict[w].tf += code # accumulate term weight
                # i = clean_string.find(t)
                # try:
                #     start = word_pos_dict[i]
                # except(KeyError):
                #     continue
                # end = start + len(t.split(" "))
                # for w in t.strip().split(" "):
                #     pos_list = self.base_text_dict[w]
                #     # print(pos_list)
                #     for i in range(len(pos_list)):
                #         if start <= pos_list[i][0] < end:
                #             self.base_text_dict[w][i][1] = code

    # code priority
    # title: 4 > headings: 3 > bold: 2 > italic: 1 > plain: 0
    def _update_format_code(self, clean_string, 
                            s_title, s_head, s_bold, s_italic):
        # print(clean_string)
        self.font_counter = {4: 0, 3: 0, 2: 0, 1: 0}
        self._format_code_helper(clean_string, s_italic, 1)
        self._format_code_helper(clean_string, s_bold, 2)
        self._format_code_helper(clean_string, s_head, 3)
        self._format_code_helper(clean_string, s_title, 4)








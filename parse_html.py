import json
from bs4 import BeautifulSoup
import re
import string
from collections import defaultdict
from gensim.parsing.porter import PorterStemmer

# file_dict = {}
# with open(path, 'rt') as f:
#     file_dict = json.load(f)


class Parser:
    def reset(self, content):
        self.content = content
        self.soup = BeautifulSoup(self.content, 'html.parser')
        self.base_text_dict = defaultdict(list)  # k:word, v:occurrencelist}
        # self.anchor_dict = {}
        self.anchor_dict = defaultdict(list)  # {k: word, v:urllist}

    def parse(self, content):
        self.reset(content)

        for tag in self.soup.find_all(['style', 'script']):
            tag.decompose()

        s_title = self._get_title()
        s_heading = self._get_heading()
        # print(s_heading)
        s_bold = self._get_bold()
        s_italic = self._get_italic()
        text = self._get_text()
        stemmed_text = self._stem_string(text)
        # print(stemmed_text)
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

    #                 if w not in self.anchor_dict:
    #                     self.anchor_dict[w] = [aUrl[i]]
    #                 else:
    #                     self.anchor_dict[w].append(aUrl[i])

    # title
    def _get_title(self):
        tTags = []
        titles = self.soup.find('title')
        if titles is not None:
            for t in titles:
                title = t.string
                tTags.append(self._stem_string(title))
        return tTags

    def _get_heading(self):
        hTags = []
        for headlings in self.soup.find_all(re.compile('^h[1-6]$')):
            heading = headlings.text.strip()
            hTags.append(self._stem_string(heading))
        return hTags

    # bold word
    def _get_bold(self):
        bTags = []
        for i in self.soup.findAll('b'):
            bold = i.text.strip()
            bTags.append(self._stem_string(bold))
        return bTags

    # italic word
    def _get_italic(self):
        iTags = []
        for i in self.soup.findAll('i'):
            italic = i.text.strip()
            iTags.append(self._stem_string(italic))
        return iTags

    # anchor word
    def _get_anchor(self):
        aTags = []
        aUrl = []
        for i in self.soup.findAll('a', href=True):
            aTags.append(self._stem_string(i.text.strip()))
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

    # input: test
    # return: cleaned base string "all word context" (NOTE: '\n'etc should be removed)

    def _stem_string(self, text):
        clean_string = ""
        for line in text.splitlines():
            for word in line.strip().lower().split(" "):
                word = word.strip(string.punctuation)
                new_word = ''
                for c in word:
                    if self._isEnglish(c):
                        new_word = new_word + c
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

        # for i in range(1, len(stem_text.split(" "))):
        #     if i != 1:
        #         char_index = char_index + word_len + 1
        #     else:
        #         char_index = word_len + 1
        #
        #     word_pos_dict[char_index] = i
        #     word_len = len(stem_text.split(" ")[i])
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




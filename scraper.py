# encoding: utf8
import re
from datetime import datetime, timedelta
import json
import scraperwiki
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors import LinkExtractor
from unidecode import unidecode

INDEX_URL = 'http://mts.ro/proiecte-legislative-in-dezbatere-publica/'

DOC_EXTENSIONS = [".docs", ".doc", ".txt", ".crt", ".xls", ".xml", ".pdf", ".docx", ".xlsx", ]

TYPE_RULES = [
    ("lege", "LEGE"),
    ("hotarare de guvern", "HG"),
    ("hotarare a guvernului", "HG"),
    ("hotarare", "HG"),
    ("hg", "HG"),
    ("ordonanta de guvern", "OG"),
    ("ordonanta de urgenta", "OUG"),
    ("ordin de ministru", "OM"),
    ("ordinul", "OM"),
]

CONTACT_TEL_FAX_PATTERN = re.compile(r'((fax|telefon|tel)[^\d]{1,10}(\d(\d| |\.){8,11}\d))')
CONTACT_EMAIL_PATTERN = re.compile(r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-]{2,5})")

# matches lines similar to "Data limită pentru primirea de propuneri/observaţii (10 zile de la publicare): 07 aprilie 2017"
# and
# "Data limită pentru primirea de propuneri/opinii/sugestii : 26.09.2016"
# and
# Perioada consultare publica: 17.12.2014 – 31.01.2015

#TODO some have this format:
#Pentru eficientizarea centralizării propunerilor/observațiilor de modificare, vă rugăm să aveţi amabilitatea de a le transmite în termen de 20 zile
#http://mts.ro/noutati/ministerului-tineretului-si-sportului-supune-dezbaterii-publice-proiectul-legii-tineretului/
#

# TODO invitatie new type?
# http://mts.ro/noutati/proiecte-legislative-in-dezbatere-publica/

FEEDBACK_DEADLINE_INFO_PATTERN = re.compile(r'data limita.*(.*\(.*de la publicare\))*.*((\d\d?\.\d\d?\.20\d\d)|(\d\d?\s[a-z]+\s20\d\d))*')
FEEDBACK_DEADLINE_DATE_PATTERN = re.compile(r'(\d\d?\.\d\d?\.20\d\d)|(\d\d?\s[a-z]+\s20\d\d)')
FEEDBACK_DEADLINE_DAYS_PATTERN = re.compile(r'\(.*de la publicare\)')
FEEDBACK_DEADLINE_PERIOD_PATTERN = re.compile(r'perioada consultare publica.*')

FEEDBACK_DATE_FORMATS = ['%d %B %Y', '%d.%m.%Y']

DIACRITICS_RULES = [
    (ur'[șş]', 's'),
    (ur'[ȘŞ]', 'S'),
    (ur'[țţ]', 't'),
    (ur'[ȚŢ]', 'T'),
    (ur'[ăâ]', 'a'),
    (ur'[ĂÂ]', 'A'),
    (ur'[î]', 'i'),
    (ur'[Î]', 'I'),
]

class Publication(scrapy.Item):
    institution = scrapy.Field()
    identifier = scrapy.Field()
    type = scrapy.Field()
    date = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()
    documents = scrapy.Field()
    contact = scrapy.Field()
    feedback_days = scrapy.Field()
    max_feedback_date = scrapy.Field()

def text_from(sel):
    return sel.xpath('string(.)').extract_first().strip()

def strip_diacritics(text):
    """
    Replace all diacritics in the given text with their regular counterparts.
    :param text: the text to look into
    :return: the text without diacritics
    """
    result = text
    for search_pattern, replacement in DIACRITICS_RULES:
        result = re.sub(search_pattern, replacement, result, re.UNICODE)
    return unidecode(result)

def guess_initiative_type(text, rules):
    """
    Try to identify the type of a law initiative from its description.
    Use a best guess approach. The rules are provided by the caller as a list
    of tuples. Each tuple is composed of a search string and the initiative
    type it matches to.
    :param text: the description of the initiative
    :param rules: the rules of identification expressed as a list of tuples
    :return: the type of initiative if a rule matches; "OTHER" if no rule
    matches
    """
    text = strip_diacritics(text)

    for search_string, initiative_type in rules:
        if search_string in text:
            return initiative_type
    else:
        return "OTHER"

def extract_documents(selector_list):
    """
    Extract white-listed documents from CSS selectors.
    Generator function. Search for links to white-listed document types and
    return all matching ones. Each entry has two properties. "type" contains
    the link text, "url" contains the link URL.
    :param selector_list: a SelectorList
    :return: a generator
    """
    for link_selector in selector_list:
        url = link_selector.css('::attr(href)').extract_first()
        if any(url.endswith(ext) for ext in DOC_EXTENSIONS):
            yield {
                'type': link_selector.css('::text').extract_first(),
                'url': url,
            }

class TineretSpider(scrapy.Spider):

    name = "tineret"
    start_urls = [INDEX_URL]

    def parse(self, response):
        articleLinks = LinkExtractor(restrict_css='div.main > div.article')
        pages = articleLinks.extract_links(response)
        for page in pages:
            yield scrapy.Request(page.url, callback=self.parse_article)

    def parse_article(self, response):
        article_node = response.css('div.main>div.article')

        title = text_from(article_node.css('h3.article-title'))
        title = self.clean_title(title)

        # clean up most of the title before checking publication type
        publication_text = title.lower().strip()
        publication_type = "OTHER"
        stop_pos = re.search(r'(pentru|privind)', publication_text)
        if stop_pos:
            publication_text_short = publication_text[0:stop_pos.start()]
            publication_type = guess_initiative_type(publication_text_short, TYPE_RULES)

        text_date = text_from(article_node.css('span.date'))
        date, date_obj = self.parse_date(text_date)

        content_node = article_node.css('div.article-content')

        description = text_from(content_node)
        description_without_diacritics = strip_diacritics(description)

        documents = [
            {
                'type': doc['type'],
                'url': response.urljoin(doc['url']),
            } for doc in
            extract_documents(content_node.css('a'))
        ]
        json_documents = json.dumps(documents)

        feedback_days, feedback_date = self.get_feedback_times(description_without_diacritics, date_obj)

        contact = self.get_contacts(description_without_diacritics)
        json_contact = json.dumps(contact)

        publication = Publication(
            institution = 'tineret',
            identifier = response.url,
            type = publication_type,
            date = date,
            title = title,
            description = description,
            documents = json_documents,
            contact = json_contact,
            feedback_days = feedback_days,
            max_feedback_date = feedback_date
        )

        scraperwiki.sqlite.save(unique_keys=['identifier'], data=dict(publication))

    def get_feedback_times(self, text, publish_date):
        fdbk_days = None
        fdbk_date = None

        text = text.strip().lower()

        phrase = re.search(FEEDBACK_DEADLINE_INFO_PATTERN, text)

        if phrase:
            #check if date is present
            date = re.search(FEEDBACK_DEADLINE_DATE_PATTERN, phrase.group(0))
            if date:
                date = date.group(0)
                for format in FEEDBACK_DATE_FORMATS:
                    try:
                        result = datetime.strptime(date, format)
                        if result:
                            fdbk_date = result
                    except ValueError:
                        pass

            # check if number of days is present
            days = re.search(FEEDBACK_DEADLINE_DAYS_PATTERN, phrase.group(0))
            if days:
                days_text = days.group(0).replace("(", "").split(" ")
                try:
                    days_int = int(days_text[0])
                    fdbk_days = days_int
                except ValueError:
                    pass

        if not fdbk_days and  not fdbk_date:
            # try with FEEDBACK_DEADLINE_PERIOD_PATTERN
            phrase = re.search(FEEDBACK_DEADLINE_PERIOD_PATTERN, text)

            if phrase:
                #check if date is present
                date = re.findall(FEEDBACK_DEADLINE_DATE_PATTERN, phrase.group(0))
                if date:
                    for format in FEEDBACK_DATE_FORMATS:
                        try:
                            result = datetime.strptime(date[-1][0], format)
                            if result:
                                fdbk_date = result
                        except ValueError:
                            pass

        if fdbk_days and not fdbk_date:
            #compute date
            fdbk_date = (publish_date + timedelta(days=fdbk_days)).date().isoformat()

        if not fdbk_days and fdbk_date:
            #compute days
            days_diff = fdbk_date - publish_date
            fdbk_days = days_diff.days

        return fdbk_days, fdbk_date

    def get_contacts(self, text):
        text = text.strip().lower()

        contact = {}

        emails = re.findall(CONTACT_EMAIL_PATTERN, text)
        contact['email'] = list(set(emails))

        numbers = re.findall(CONTACT_TEL_FAX_PATTERN, text)
        for number in numbers:
            key = number[1]
            value = number[2].replace(' ','').replace('.', '')
            if key in contact:
                contact[key].push(value)
            else:
                contact[key] = [value]

        for k,v in contact.items():
            contact[k] = ','.join(v)

        return contact

    def parse_date(self, text):
        try:
            date_obj = datetime.strptime(text, '%d.%m.%Y')
            date = date_obj.date().isoformat()
        except ValueError:
            date = None
        return date, date_obj

    def clean_title(self, text):
        """
        Remove possible extra spaces in title (ex. HOTĂRÂRE spelled as H O T Ă R Â R E)
        """
        idx = 0
        parts = text.split()
        for i in range(len(parts)):
            if len(parts[i]) > 1:
                idx = i
                break

        text = '%s %s' % (''.join(parts[:idx]), ' '.join(parts[idx:]))
        return text

process = CrawlerProcess()
process.crawl(TineretSpider)
process.start()

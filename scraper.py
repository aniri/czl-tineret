# encoding: utf8
import re
import datetime
#import json
import scraperwiki
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors import LinkExtractor

INDEX_URL = 'http://mts.ro/proiecte-legislative-in-dezbatere-publica/'

CONTACT_TEL_FAX_PATTERN = re.compile(r'((fax|telefon|tel)[^\d]{1,10}(\d(\d| |\.){8,11}\d))')
CONTACT_EMAIL_PATTERN = re.compile(r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-]{2,5})")

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

DIACRITICS_RULES = [
    (r'[șş]', 's'),
    (r'[ȘŞ]', 'S'),
    (r'[țţ]', 't'),
    (r'[ȚŢ]', 'T'),
    (r'[ăâ]', 'a'),
    (r'[ĂÂ]', 'A'),
    (r'[î]', 'i'),
    (r'[Î]', 'I'),
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
    return result

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
        #json_documents = json.dumps(documents)

        feedback_days = None
        feedback_date = self.get_feedback_date(description_without_diacritics)
        if feedback_date:
            days_diff = feedback_date - date_obj
            feedback_days = days_diff.days

        contact = self.get_contacts(description_without_diacritics)
        #json_contact = json.dumps(contact)

        publication = Publication(
            institution = 'tineret',
            identifier = self.slugify(title)[0:127],
            type = publication_type,
            date = date,
            title = title,
            description = description,
            #documents = json_documents,
            #contact = json_contact,
            #feedback_days = feedback_days,
            #max_feedback_date = feedback_date
        )

        scraperwiki.sqlite.save(unique_keys=['identifier'], data=dict(publication))

    def slugify(self, text):
        text = strip_diacritics(text).lower()
        return re.sub(r'\W+', '-', text)

    def get_feedback_date(self, text):
        formats = ['%d %B %Y', '%d.%m.%Y']
        text = text.strip().lower()

        phrase = re.search(r'data limita.*((\d\d?\.\d\d?\.20\d\d)|(\d\d?\s[a-z]+\s20\d\d))', text)

        if phrase:
            date = re.search(r'(\d\d?\.\d\d?\.20\d\d)|(\d\d?\s[a-z]+\s20\d\d)', phrase.group(0))

            if date:
                date = date.group(0)
                for format in formats:
                    try:
                        result = datetime.datetime.strptime(date, format)
                        if result:
                            return result
                    except ValueError:
                        pass

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
            date_obj = datetime.datetime.strptime(text, '%d.%m.%Y')
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

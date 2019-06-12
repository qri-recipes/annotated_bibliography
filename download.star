load("http.star", "http")
load("bsoup.star", "bsoup")
load("re.star", "re")

# download is a special function that provides HTTP access so we can fetch  data 
# from the web.
# qri will call download when saving a dataset version. download is always 
# called *before* transform
def download(ctx):
  # fetch is a list of new urls to grab
  fetch = ctx.get_config("urls")
  # if we don't have a urls array, stop the transform
  if fetch == None:
    return []

  print("fetching %d urls\n" % len(fetch))
  # fetch new urls, adding them to the end of the list, matching our schema
  return [{ "article" : extract(url), "annotations": { "notes" : "", "rating" : 0 } } for url in fetch]


# transform is a special function for modifying a dataset
# any modifications made to the passed in "ds" will be included in the commit
# qri will call transform when saving a dataset version, passing in the
# current dataset version as "ds"
def transform(ds, ctx):
  # get the previous dataset body if one exists, defaulting to 
  # an array if no body is specified
  body = ds.get_body(default=[])

  # the articles we've downloaded already are placed in ctx.download
  fetched_articles = ctx.download

  # filter out any URLs that have been fetched already, leaving a list of 
  # urls to add
  additions = new_urls(body, fetched_articles)

  # append additions to the body
  body = body + additions

  # set dataset structure:
  ds.set_structure(structure)

  # update the dataset with new data by calling set_body:
  ds.set_body(body)




"""
helper functions:
"""
def new_urls(body, fetched):
  return [entry for entry in fetched if missing_url(body, entry['article']['url'])]

def missing_url(body, url):
  for citation in body:
    if citation.get('article', {}).get('url', '') == url:
      return False
  return True

# extract fetches a url, with a special case if it's an arxiv.org url
def extract(url):
  res = http.get(url)
  if res.status_code != 200:
    error("bad response: %d" % res.status_code)
  soup = bsoup.parseHtml(res.body())

  if len(re.findall('^http://arxiv.org', url)) > 0 or len(re.findall('^https://arxiv.org', url)) > 0:
    return extract_arxiv(url, soup)

  return extract_url(url, soup)


def extract_url(url, soup):
  return {
    'url' : url,
    'about' : '',
    'author' : '',
    'name' : soup.find('title').get_text(),
    'description' : '',
  }

def extract_arxiv(url, soup):
  return {
    'url' : url,
    'about' : '',
    'author' : '',
    'name' : remove_prefix(soup.find('h1', { 'class': 'title'}).get_text(), "Title:"),
    'description' : remove_prefix(soup.find('blockquote', { 'class': 'abstract'}).get_text(), "Abstract:"),
  }

def remove_prefix(s, prefix):
    return s[len(prefix):] if s.startswith(prefix) else s


# We've worked out this data model for you using standards for interoperability
# by schema.org. 
# This is going to look scary & big, because data modeling is a whole field of
# study! You don't need to fully understand schemas to work with data,
# and this is a common situation, where a schema will be provided to you by
# someone else.
#
# This schema is set to "strict", meaning it will reject any data that doesn't
# validate
structure = {
    "format": "json",
    "strict": True,
    "schema": {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "article": {
                    "$ref": "#/definitions/ScholarlyArticle"
                },
                "annotations": {
                    "oneOf": [
                        {
                            "$ref": "#/definitions/Annotation"
                        },
                        {
                            "type": "array",
                            "items": {
                                "$ref": "#/definitions/Annotation"
                            }
                        }
                    ]
                }
            }
        },
        "definitions": {
            "ScholarlyArticle": {
                "type": "object",
                "required": [
                    "url"
                ],
                "properties": {
                    "about": {
                        "title": "about",
                        "description": "The subject matter of the content",
                        "oneOf": [
                            {
                                "type": "array",
                                "items": {
                                    "type": "string"
                                }
                            },
                            {
                                "type": "string"
                            }
                        ]
                    },
                    "author": {
                        "title": "author",
                        "description": "",
                        "oneOf": [
                            {
                                "type": "array",
                                "items": {
                                    "type": "string"
                                }
                            },
                            {
                                "type": "string"
                            }
                        ]
                    },
                    "wordCount": {
                        "title": "wordCount",
                        "type": "integer",
                        "description": "The number of words in the text of the Article."
                    },
                    "name": {
                        "title": "name",
                        "type": "string",
                        "description": "The name of the item."
                    },
                    "description": {
                        "title": "description",
                        "type": "string",
                        "description": "A description of the item."
                    },
                    "identifier": {
                        "title": "identifier",
                        "type": "string",
                        "description": "Text or URL The identifier property represents any kind of identifier for any kind of Thing, such as ISBNs, GTIN codes, UUIDs etc. Schema.org provides dedicated properties for representing many of these, either as textual strings or as URL (URI) links. See background notes for more details."
                    },
                    "url": {
                        "title": "url",
                        "type": "string",
                        "description": "URL of the item"
                    }
                }
            },
            "Annotation": {
                "type": "object",
                "properties": {
                    "author": {
                        "title": "author",
                        "type": "string",
                        "description": "author of the annotation"
                    },
                    "rating": {
                        "title": "rating",
                        "type": "number",
                        "description": "a 0-5 rating of the relevance of the article",
                        "minimum": 0,
                        "maximum": 5
                    },
                    "read": {
                        "title": "read",
                        "type": "boolean",
                        "description": "weather or not the author has read this article"
                    },
                    "notes": {
                        "title": "notes",
                        "type": "string",
                        "description": "notes from the author"
                    }
                }
            },
        }
    }
}
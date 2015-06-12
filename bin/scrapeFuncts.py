### Importing libraries
from random import choice
from time import sleep
from bs4 import BeautifulSoup
import requests, os, pdb, re
from time import strptime
from numpy.random import randn
from numpy import arange
from notify import *
from pyspark.sql.types import *
from io import open


# Function for getting Beautiful Soup
def getSoup(link,user_agent, parser = 'html5lib'):

    # Pausing for random time
    header = { 'User-Agent' : user_agent}
    sArray = arange(0,.5,.05)
    wait   = choice(sArray) + abs(randn(1)*.25)
    sleep(wait)

    # Reading page
    r = requests.get(link, headers = header)
    if r.status_code == requests.codes.ok:
        return BeautifulSoup(r.text,parser)
    else:
        print 'HTTP Status Code: ' + str(r.status_code)
        return []


# Function to get region identifiers
def regionIds(soup, include = None):

    # Getting links for regions, excluding the city links
    city_links = [tag['href'] for tag in soup.select('p a[href]')]
    region_links = [tag['href'] for tag in soup.findAll('a', href = re.compile('^/breweries/')) \
         if tag['href'] not in city_links]

    # Region identifiers
    region_ids = []
    for link in region_links:
        ids = re.search(r"/breweries/(.*)/(\d+)/(\d+)/", link)
        if include:
            if ids.group(3) in include: region_ids.append([ids.group(1), ids.group(2), ids.group(3)])
        else:
            region_ids.append([ids.group(1), ids.group(2), ids.group(3)])

    return region_ids


# Function to get brewery identifiers
def breweryIds(link, region_ids, user_agents):

    # Looping through regions
    brewery_ids = []
    for region_id in region_ids:

        # Getting tables of active and inactive breweries within a region
        url_region = link + 'breweries/' + '/'.join(region_id) + '/'
        soup = getSoup(url_region, choice(user_agents))
        tables = soup.findAll('table', id = 'brewerTable')
        for i, table in enumerate(tables):

            # Identifying active and inactive breweries, pausing if there are more than two
            # tables. The first table should contain active breweries and the second inactive
            open_brewery = True if i == 0 else False
            if i > 1: pdb.set_trace()
            tags = table.findAll('a', href = re.compile(r"^/brewers/.*"))

            # Brewery Identifiers
            for tag in tags:
                ids = re.search(r"/brewers/(.*)/(\d*)/",tag['href'])
                brewery_ids.append([ids.group(2), ids.group(1), open_brewery, tag.text])

    return brewery_ids


# Function to get remaining brewery features and a list of beers
def beerIds(link, brewery_id, user_agent):

    # Getting soup
    url = link + 'brewers/' + '/'.join(brewery_id[1::-1]) + '/'
    soup = getSoup(url, user_agent)

    # Brewery features (still need number of beers)
    address = featBrewery(soup)
    brewery_id.extend(address)
    brewery_id[0] = int(brewery_id[0])

    # List of beers on brewery landing page
    def addIds(soup, list_ids):
        tags = soup.find_all("a", href = re.compile(r"^/beer/(?!(rate|top-50)).*\d+"))
        for tag in tags:
            ids = re.search(r"/beer/(.*)/(\d+)/", tag['href'])
            list_ids.append([ids.group(2), ids.group(1), tag.text])
        return list_ids
    beer_ids = addIds(soup, [])

    # Checking whether additional pages exist. If so, adding to list of beer ids
    pages = set([tag['href'] for tag in soup.select("p.pagination a[href]") if \
        re.search(r".*(?<!/0/1/)$",tag['href'])])
    if pages:
        for page in pages:
            url = link + page
            soup = getSoup(url, user_agent)
            beer_ids = addIds(soup,beer_ids)

    return (brewery_id, beer_ids)


# Function to loop through pages of reviews for a beer
def loopReviews(link, beer_id, brewery_id, user_agents):

    # Getting Soup
    suffix = '/'.join(beer_id[1::-1]) + '/'
    url = link + 'beer/' + suffix
    beer_id[0] = int(beer_id[0])
    soup = getSoup(url, choice(user_agents))

    # Exiting if current beer is an alias
    alias = temp = re.search(r"The brewer markets this same or near-same product " + \
            r"by more than one names", soup.text)
    if alias:
        return (None, None)

    # Beer and review features on landing page
    beer_features = beer_id + [brewery_id] + featBeer(soup)
    reviews = tryReview(beer_id[0], url, user_agents)

    # Determining whether there are multiple pages of reviews. If so, looping through them
    page_links = soup.findAll('a', attrs = {"class":"ballno"}, href = re.compile(r"^/beer/.*"))
    if page_links:

        # Finding the number of pages
        r = re.search(r"/beer/" + suffix + r"1/(\d+)/", page_links[-1]['href'])
        n_pages = int(r.group(1))

        # Looping through pages
        for i in range(2, n_pages + 1):
            url_i = url + '/1/' + str(i) + '/'
            reviews_i = tryReview(beer_id[0], url_i, user_agents)
            reviews.extend(reviews_i)

    return (beer_features, reviews)


# Wrapper for featReviews which first tries the xml parser and then the html parser
def tryReview(beer_id, url, user_agents):

    # Writing progress to file
    with open("./last_update.txt",'w', encoding='utf8') as file:
        file.write(url)

    # Trying HTML5 parser first
    try:
        soup = getSoup(url, choice(user_agents), 'html5lib')
        reviews = featReviews(soup, beer_id)
    except:

        # Trying lxml's HTML parser second
        try:
            soup = getSoup(url, choice(user_agents), 'lxml')
            reviews = featReviews(soup, beer_id)
        except:
            reviews = []

    return reviews


# Function for retrieving brewery features
def featBrewery(soup):

    # Address features
    street_address = itemProp(soup, "streetAddress")
    city           = itemProp(soup, "addressLocality")
    region     = itemProp(soup, "addressRegion")
    country        = itemProp(soup, "addressCountry")
    postal_code    = itemProp(soup, "postalCode")
    return [country, region, city, street_address, postal_code]


# Function for retrieving beer features
def featBeer(soup):

    # Functions
    def reScore(soup, element, phrase, regex):
        result = soup.find(element, title = re.compile(phrase))
        score = re.search(regex, result['title']) if result else ''
        return score.group(0) if score else ''

    # Scores
    score_over  = num(reScore(soup, "div", r".*relative to all beers.*", r"^\d+.\d+"), float)
    score_style = num(reScore(soup, "div", r".*relative to its peers.*", r"^\d+.\d+"), float)
    score_wavg  = num(reScore(soup, "a", r".*Bayesian mean of all.*", r"\d+.\d+"), float)
    score_avg   = num(reSearch(soup.text, r"MEAN: ([.\d]+)/5.0"), float)

    # Beer descriptors and other characteristics
    ibu      = num(reSearch(soup.text, r"IBU: (\d+)"), int)
    calories = num(reSearch(soup.text, r"EST. CALORIES: ([,\d]+)"), int)
    abv      = num(reSearch(soup.text, r"ABV: ([.\d]+)%"), float)
    style    = soup.find("a", href = re.compile(r"/beerstyles/.*")).get_text()
    n_review = num(itemProp(soup, "count"), int)

    # Retired beer?
    r = reSearch(soup.text, r"(\(RETIRED\))")
    retired = True if r else False

    return [score_over, score_style, score_wavg, score_avg, style, n_review, ibu,
            calories, abv, retired]


# Function for retrieving review features
def featReviews(soup, beer_id):

    # Positioning the soup at the table which contains the reviews
    s = soup.find("table", attrs = {"style": "padding: 10px;"})

    # Looping through reviews
    d_reviews = []
    while s.findNext('div', attrs = {"style":"padding: 0px 0px 0px 0px;"}):

        # Score features
        s = s.findNext('div', attrs = {"style":"padding: 0px 0px 0px 0px;"})
        t = s.text.replace(u'\xa0',u'')
        score         = float((reSearch(t, r"^\s*([.\d]+)")))
        score_aroma   = int((reSearch(t, r"AROMA\s+(\d+)/10")))
        score_appear  = int((reSearch(t, r"APPEARANCE\s+(\d)/5")))
        score_taste   = int((reSearch(t, r"TASTE\s+(\d+)/10")))
        score_palate  = int((reSearch(t, r"PALATE\s+(\d)/5")))
        score_overall = int((reSearch(t, r"OVERALL\s+(\d+)/20")))

        # User number
        s = s.findNext("small", attrs = {"style":"color: #666666; font-size: 12px; font-weight: bold;"})
        r = re.search(r"/\w+/(\d+)/",s.find('a')['href'])
        user_num = int(r.group(1)) if t else None

        # Review date
        regex = r"(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+\d+,\s+\d{4}"
        r = re.search(regex, s.text.replace(u'\xa0',u''))
        date = r.group(0) if r else ''

        # Review body
        s = s.findNext("div", attrs = {"style":"padding: 20px 10px 20px 0px; " +
            "border-bottom: 1px solid #e0e0e0; line-height: 1.5;"})
        body = s.text if s else ''

        d_reviews.append([beer_id, user_num, date, score, score_aroma, score_appear,
            score_taste, score_palate, score_overall, body])

    return d_reviews


# Function to write Brewery RDD to Parquet file
def writeRDD(rddName, rddData, sqlContext, overwrite = False):

    # Defining and applying field names and schema
    dict_fields = {
        'breweries':[('id_brewery', IntegerType()), ('id_link', StringType()),
            ('is_open', BooleanType()), ('name', StringType()),
            ('country', StringType()), ('region', StringType()),
            ('city', StringType()), ('street_address', StringType()),
            ('postal_code', StringType())],
        'beers':[('id_beer', IntegerType()), ('id_link', StringType()),
            ('beer_name', StringType()), ('id_brewery', IntegerType()),
            ('score_overall', FloatType()), ('score_style', FloatType()),
            ('score_bwavg', FloatType()), ('score_avg', FloatType()),
            ('style', StringType()), ('n_reviews', IntegerType()),
            ('ibu', IntegerType()), ('calories', IntegerType()),
            ('abv', FloatType()), ('is_retired', BooleanType())],
        'reviews': [('id_beer', IntegerType()), ('id_user', IntegerType()),
            ('date',StringType()), ('score',FloatType()),
            ('score_aroma',IntegerType()), ('score_appear',IntegerType()),
            ('score_taste',IntegerType()), ('score_palate',IntegerType()),
            ('score_overall',IntegerType()), ('review_text', StringType())]
        }
    fields = [StructField(field[0], field[1], True) for field in dict_fields[rddName]]
    schema = StructType(fields)
    df = sqlContext.createDataFrame(rddData, schema)

    # Writing, appending or updating file
    rddName = '../data/' + rddName
    if overwrite:
        df.select("*").save(rddName + '.parquet', 'parquet', 'overwrite')
    else:
        df.select("*").save(rddName + '.parquet', 'parquet', 'append')


# Function to get item properties
def itemProp(soup, prop):
    result = soup.find(itemprop = prop)
    return result.text.strip() if result else ''


# Function to convert string to numbers
def num(s, num_type):
    try:
        if num_type == int:
            return int(s)
        elif num_type == float:
            return float(s)
    except ValueError:
        return None


# Function to search for single expression
def reSearch(text, regex):
    result = re.search(regex, text)
    return result.group(1) if result else ''

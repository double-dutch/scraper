import sys, argparse, pdb, csv, time
from scrapeFuncts import *
from notify import *
from pyspark import SparkContext
from pyspark.sql import SQLContext


def scrape(link = ''):

    # Starting Spark instance
    sc = SparkContext("local", "Scraper")
    sqlContext = SQLContext(sc)
    overwrite = True

    # Calling list of User-Agents
    with open('../data/user_agents.txt','r') as file:
        reader = csv.reader(file, delimiter = '\t')
        user_agents = [line[0] for line in reader]

    # Identifiers for breweries in each region
    url = link + 'breweries/'
    soup = getSoup(url, choice(user_agents))
    region_ids = regionIds(soup, ['213'])
    brewery_ids = breweryIds(link, region_ids[0:3], user_agents)

    # Looping through breweries, getting (1) complete brewery features, (2) A list of beer
    # features and (3) a list of review features
    for brewery_id in brewery_ids:

        # Remaining brewery features and a list of beers
        brew_features, beer_ids = beerIds(link, brewery_id, choice(user_agents))

        # Remaining beer features and review features
        beer_features = []
        review_features = []
        for beer_id in beer_ids:
            beer, reviews = loopReviews(link, beer_id, brew_features[0], user_agents)
            if beer: beer_features.append(beer)
            if reviews: review_features.extend(reviews)

        # Writing data to dictionary
        writeRDD('breweries', [brew_features], sqlContext, overwrite)
        if beer_features: writeRDD('beers', beer_features, sqlContext, overwrite)
        if review_features: writeRDD('reviews', review_features, sqlContext, overwrite)
        overwrite = False

    # Stopping SparkContext
    sc.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('link', help="Website to scrape", type = str, default = '')
    args = parser.parse_args()
    scrape(args.link)

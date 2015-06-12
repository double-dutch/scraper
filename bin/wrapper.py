### Importing libraryies
from scraper import *
from notify import *
import traceback, sys


# Wrapper function
def wrap(overwrite,start_input):


    ### Calling last update
    try:
        with open("../data/last_update.txt", "r") as f:
            start_last = int(f.readlines()[0]) + 1
    except:
        start_last = 'last_update.txt could not be found'

    # Choosing start index
    if overwrite == 1:
        start = start_input
        print 'User opted to overwrite existing file'
    else:
        start = start_last
        print 'User opted to append to previous file. Beginning from ' + str(start)


    ### Running scraper
    try:
        scrape(start,38000,'../data/data_reviews','../data/data_reviews',overwrite,1)
    except Exception, err:
	print traceback.format_exc()
        email_update('Scraper broke','andrew.howland@gmail.com','ssycpklekdbywktq')



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('overwrite', help="Overwrite existing data", \
        nargs='?', type=int, default=0, choices=[0,1])
    parser.add_argument("start", type=int, help="Starting brewery index", default = 1)
    args = parser.parse_args()
    wrap(args.overwrite, args.start)

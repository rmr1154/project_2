from splinter import Browser
from bs4 import BeautifulSoup
import requests
import pymongo
import pandas as pd



def scrape():
    executable_path = {'executable_path': 'chromedriver.exe'}
    browser = Browser('chrome', **executable_path, headless=False)
    
    latest_news = {}
    
    '''Scrape the NASA Mars News Site and collect the latest News Title and Paragraph Text. 
    Assign the text to variables that you can reference later.'''

    nasa = 'https://mars.nasa.gov/news/'
    browser.visit(nasa)
    html = browser.html
    soup = BeautifulSoup(html, 'html.parser')

    news = soup.find('li', class_='slide')

    news_title = news.find('h3').text
    news_p = news.find('div', class_='article_teaser_body').text

    #print(news_title)
    #print(news_p)

    latest_news['news_title'] = news_title
    latest_news['news_p'] = news_p
    '''Visit the url for JPL Featured Space Image here.

    Use splinter to navigate the site and find the image url for the current Featured Mars Image and 
    assign the url string to a variable called featured_image_url.

    Make sure to find the image url to the full size .jpg image.

    Make sure to save a complete url string for this image.'''

    jpl = 'https://www.jpl.nasa.gov/spaceimages/?search=&category=Mars'
    browser.visit(jpl)
    html = browser.html
    soup = BeautifulSoup(html, 'html.parser')

    tag = soup.find('a', {"id": "full_image"}).get('data-link')

    browser.visit(f"https://www.jpl.nasa.gov{tag}")
    html = browser.html
    soup = BeautifulSoup(html, 'html.parser')

    soup = soup.find('figure', class_='lede')
    tag = soup.find('a').get('href')
    featured_image_url = f"https://www.jpl.nasa.gov{tag}"

    #print(featured_image_url)

    latest_news['featured_image_url'] = featured_image_url

    '''Visit the Mars Weather twitter account here and scrape the latest Mars weather tweet from the page. 
    Save the tweet text for the weather report as a variable called mars_weather.'''

    url = 'https://twitter.com/marswxreport?lang=en'
    browser.visit(url)
    html = browser.html
    soup = BeautifulSoup(html, 'html.parser')

    tag = soup.find('p', class_="TweetTextSize")
    tag.a.decompose()
    
    mars_weather = tag.text
    #print(mars_weather)

    latest_news['mars_weather'] = mars_weather

    '''Visit the Mars Facts webpage here and use Pandas to scrape the table containing facts about the 
    planet including Diameter, Mass, etc.

    Use Pandas to convert the data to a HTML table string.'''

    url = 'https://space-facts.com/mars/'
    tables = pd.read_html(url)
    tables = tables[0]
    tables.columns=['Fact','Value']
    tables.set_index('Fact',inplace=True)

    mars_facts = tables.to_html(bold_rows=True, index=True, index_names=False)
    #tables

    latest_news['mars_facts'] = mars_facts

    '''Visit the USGS Astrogeology site here to obtain high resolution images for each of Mar's hemispheres.

    You will need to click each of the links to the hemispheres in order to find the image url to the full resolution image.

    Save both the image url string for the full resolution hemisphere image, and the Hemisphere title containing the hemisphere name. Use a Python dictionary to store the data using the keys img_url and title.

    Append the dictionary with the image url string and the hemisphere title to a list. This list will contain one dictionary for each hemisphere.

    # Example:
    hemisphere_image_urls = [
        {"title": "Valles Marineris Hemisphere", "img_url": "..."},
        {"title": "Cerberus Hemisphere", "img_url": "..."},
        {"title": "Schiaparelli Hemisphere", "img_url": "..."},
        {"title": "Syrtis Major Hemisphere", "img_url": "..."},
    ]
    '''
    usgs = 'https://astrogeology.usgs.gov/search/results?q=hemisphere+enhanced&k1=target&v1=Mars'
    browser.visit(usgs)
    html = browser.html
    soup = BeautifulSoup(html, 'html.parser')

    tags = soup.find_all('div', class_='item')
    hemisphere_image_urls = []
    for item in tags:
        href = item.find('a', href=True).get('href')
        title = item.find('div', class_='description')
        title = title.find('a').text
        #print(title)
        browser.visit(f"https://astrogeology.usgs.gov{href}")
        html = browser.html
        soup = BeautifulSoup(html, 'html.parser')
        img_url = soup.find('img', class_="wide-image").get('src')
        img_url = f"https://astrogeology.usgs.gov{img_url}"
        #print(img_url)
        hemisphere_image_urls.append({"title":title,"img_url":img_url})

        latest_news['hemisphere_image_urls'] = hemisphere_image_urls

    return latest_news
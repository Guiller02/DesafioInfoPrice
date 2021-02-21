# Imports

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep
import pandas as pd


# Important things:
# ----------------
# Content of robots.txt:

# User-agent: *
# Disallow: /avaliacao
# Disallow: /parceiros
# Disallow: /product-description
# Disallow: /portal
# Disallow: /garantia
# Disallow: /estaticapop
# Disallow: /search
# Disallow: /incluir_prime
# Disallow: /*cage.html
#
# Disallow: */g/*
# Disallow: */f/*
# Disallow: */m/*
# Disallow: */l/*
# Disallow: */c/*
# Disallow: */tag-imutavel*
# Disallow: */*filtro=*
# Disallow: *{"id":*{"id":*{"id":*{"id":*
#
# #1
# #Mon Nov 09 17:30:00 2020


# as we are only using the https://www.americanas.com.br/categoria/tv-e-home-theater/tv/pagina-1' and the product
# page (not /product-description), this site does not disallows this script to scrapy her data even for commerce issues

class Scrapy:
    _base_url = ''
    _driver = ''
    _links = []

    # Array of contents in the product
    _gtins = []
    _descriptions = []
    _prices = []
    _urls = []
    _url_pictures = []

    # Dict to create the dataframe
    _dict = dict

    # Final dataframe
    americanas_dataframe = ''

    # Starting the class, receiving the url that we will scrap
    def __init__(self, url):
        self._base_url = url

    # The main method, that will call the others methods
    def scrapy(self):
        self._root_request()
        self._product_request()
        self._create_dataframe()

    # private method, that will locally download the webdriver for chrome if its not installed and then run the page
    # parameter
    def _get_request(self, url):
        # in each time this method is called, this script will sleep 10 seconds, to not get banned from americanas or
        # overload his server

        sleep(10)
        self._driver = webdriver.Chrome(ChromeDriverManager().install())
        self._driver.get(url)

    # First request, in the root page
    def _root_request(self):
        self._get_request(self._base_url)
        # search all products
        products = self._driver.find_elements_by_class_name('product-grid-item')

        # in each product, take the url and then append to _links
        for _ in products:
            self._links.append(_.find_element_by_class_name('Link-bwhjk3-2').get_attribute('href'))

        self._driver.close()

    def _product_request(self):
        # Unfortunately, all links in the site have the same class, so to not take others contents that is not
        # on the pagination, i will take the 24 positions (4 columns x 6 rows)
        position = 1
        for _ in self._links[0:24]:

            print('Page: ' + self._base_url[-1])

            print('Scraping product', position, 'of 24')
            self._get_request(_)

            # to see if the product its market place, if is, will only close the chrome

            if 'Este produto é vendido e entregue por Americanas.' in self._driver.find_element_by_class_name(
                    'offers-box__Wrapper-sc-189v1x3-0').text:
                # because the gtin does not have an id or a unique class, we need to take all content in the table
                # and then take the second position and then clean the string to append only the id

                gtin = self._driver.find_elements_by_class_name('src__View-sc-70o4ee-7')

                self._gtins.append(gtin[1].text.replace('Código de barras', ''))

                self._descriptions.append(self._driver.find_element_by_class_name('src__Text-sc-154pg0p-0').text)

                self._prices.append(self._driver.find_element_by_class_name('src__BestPrice-sc-1jvw02c-5').text)

                # how we are already using the url in the loop, have no need to search the url in the site

                self._urls.append(_)

                self._url_pictures.append(
                    self._driver.find_element_by_class_name('src__Image-xr9q25-0').get_attribute('src'))

                # print of the last content in the array
                print(self._gtins[-1])

                print(self._descriptions[-1])

                print(self._prices[-1])

                print(self._urls[-1])

                print(self._url_pictures[-1])
            position += 1
            # closing the chrome and then quiting to not throw a error after finishing the script
            self._driver.close()

        self._driver.quit()

    def _create_dataframe(self):
        # creating a dict to create a dataframe
        self._dict = {
            'gtin': self._gtins
            , 'description': self._descriptions
            , 'price': self._prices
            , 'url': self._urls
            , 'picture': self._url_pictures
        }

        self.americanas_dataframe = pd.DataFrame(self._dict)


# to test the script
if __name__ == '__main__':
    sc = Scrapy('https://www.americanas.com.br/categoria/tv-e-home-theater/tv/pagina-1')
    sc.scrapy()

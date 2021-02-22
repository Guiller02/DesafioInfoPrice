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
    _present_page = 1
    _number_of_pages = 99999
    _output_location = ''
    _have_number_of_page = False

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
    def __init__(self, url, output_location):
        print('Starting Scrapy process')
        self._base_url = url
        self._output_location = output_location

    # The main method, that will call the others methods
    def scrapy(self):
        self._product_request()

    # Get the number of pages the root have, to make a loop

    def _get_number_of_pages(self):
        pagination = self._driver.find_elements_by_class_name('pagination-product-grid')

        number_of_pages = pagination[-1].text

        self._number_of_pages = number_of_pages[-1]

        # Because if does not check this, the last _number_of_page will be the currently page
        self._have_number_of_page = True

    # private method, that will locally download the webdriver for chrome if its not installed and then run the page
    # parameter
    def _get_request(self, url):
        # in each time this method is called, this script will sleep 10 seconds, to not get banned from americanas or
        # overload his server

        # Sleep 10 seconds in every request

        self._driver = webdriver.Chrome(ChromeDriverManager().install())
        self._driver.get(url)

        # Just to make sure the site loads before the searchs in selenium and to make a pause between each request
        sleep(10)

    # First request, in the root page
    def _root_request(self):
        self._get_request(self._base_url + str(self._present_page))
        # search all products
        products = self._driver.find_elements_by_class_name('product-grid-item')

        if (self._have_number_of_page == False):
            self._get_number_of_pages()

        # in each product, take the url and then append to _links

        links = []
        for _ in products:
            links.append(_.find_element_by_class_name('Link-bwhjk3-2').get_attribute('href'))

        self._links = links

        self._driver.close()

    def _product_request(self):

        # if you want to scrapy specific page to the last page, remove the comment where the comment have 2 #

        ## i = 4
        # Pagination and position to monitoring where the program i
        while self._present_page <= int(self._number_of_pages):

        ## while i <= int(self._number_of_pages):

            gtins = []

            descriptions = []

            prices = []

            urls = []

            url_pictures = []

            position = 1

            ## self._present_page = i
            # In each root page (in the pagination), will request each product individually
            self._root_request()

            # Unfortunately, all links in the site have the same class, so to not take others contents that is not
            # on the pagination, i will take the 24 positions (4 columns x 6 rows)
            for _ in self._links[0:24]:

                print('Page: ', self._present_page, 'of : ', self._number_of_pages)

                print('Scraping product', position, 'of 24')
                self._get_request(_)

                # to see if the product its market place, if is, will only close the chrome

                if 'Este produto é vendido e entregue por Americanas.' in self._driver.find_element_by_class_name(
                        'offers-box__Wrapper-sc-189v1x3-0').text:
                    # because the gtin does not have an id or a unique class, we need to take all content in the table
                    # and then take the second position and then clean the string to append only the id

                    gtin = self._driver.find_elements_by_class_name('src__View-sc-70o4ee-7')

                    gtins.append(gtin[1].text.replace('Código de barras', '').strip())

                    descriptions.append(
                        self._driver.find_element_by_class_name('src__Text-sc-154pg0p-0').text)

                    price = self._driver.find_element_by_class_name('src__BestPrice-sc-1jvw02c-5').text

                    prices.append(price.replace('R$ ', ''))

                    # how we are already using the url in the loop, have no need to search the url in the site

                    urls.append(_)

                    url_pictures.append(
                        self._driver.find_element_by_class_name('src__Image-xr9q25-0').get_attribute('src'))

                    # print of the last content in the array
                    print(gtins[-1])

                    print(descriptions[-1])

                    print(prices[-1])

                    print(urls[-1])

                    print(url_pictures[-1])

                    print('---------------------')

                self._gtins = gtins

                self._descriptions = descriptions

                self._prices = prices

                self._urls = urls

                self._url_pictures = url_pictures

                self._create_dataframe()
                self._write_dataframe()

                position += 1

                # closing the chrome and then quiting to not throw a error after finishing the script
                self._driver.close()

            ## i +=1



            #self._present_page = self._present_page + 1

            # Sleep 5 minutes, to not overload the americanas server
            print('Getting to next page')
            sleep(300)
        # After finishing the scrap, quit the chrome driver, to not throw an error
        self._driver.quit()
        print('Finished Scrapy process')

    # Create Write dataframe containing all products

    def _create_dataframe(self):
        print('Starting Load process')
        # creating a dict to create a dataframe
        self._dict = {
            'gtin': self._gtins
            , 'description': self._descriptions
            , 'price': self._prices
            , 'url': self._urls
            , 'picture': self._url_pictures
        }

        self.americanas_dataframe = pd.DataFrame(self._dict)

    def _write_dataframe(self):
        self.americanas_dataframe.to_csv(
            self._output_location + '/scrapy_output/americanas_' + str(self._present_page) + '.csv',
            sep=';', index=False)

        print('Finished writing americanas'+str(self._present_page)+'.csv')
        print('---------------------------------')


# to test the script
if __name__ == '__main__':
    sc = Scrapy('https://www.americanas.com.br/categoria/tv-e-home-theater/tv/pagina-', '../../data/output/')
    sc.scrapy()

from .. import difference_detection_processor
from ..exceptions import ProcessorException
from . import Restock
from loguru import logger

import urllib3
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
name = 'iTunes Availablity & Price detection'
description = 'Detects if the product is available and monitors price changes'

class UnableToExtractRestockData(Exception):
    def __init__(self, status_code):
        # Set this so we can use it in other parts of the app
        self.status_code = status_code
        return

class MoreThanOnePriceFound(Exception):
    def __init__(self):
        return

class MoreThanOneResultReturned(Exception):
    def __init__(self):
        return
        
def _search_prop_by_value(matches, value):
    for properties in matches:
        for prop in properties:
            if value in prop[0]:
                return prop[1]  # Yield the desired value and exit the function

def _deduplicate_prices(data):
    import re

    '''
    Some price data has multiple entries, OR it has a single entry with ['$159', '159', 159, "$ 159"] or just "159"
    Get all the values, clean it and add it to a set then return the unique values
    '''
    unique_data = set()

    # Return the complete 'datum' where its price was not seen before
    for datum in data:

        if isinstance(datum.value, list):
            # Process each item in the list
            normalized_value = set([float(re.sub(r'[^\d.]', '', str(item))) for item in datum.value if str(item).strip()])
            unique_data.update(normalized_value)
        else:
            # Process single value
            v = float(re.sub(r'[^\d.]', '', str(datum.value)))
            unique_data.add(v)

    return list(unique_data)

def get_appletv_availability(html_content, hd_price = False) -> Restock:
    """
    Kind of funny/cool way to find price/availability in one many different possibilities.
    Use 'extruct' to find any possible RDFa/microdata/json-ld data, make a JSON string from the output then search it.
    """
    from bs4 import BeautifulSoup
    from jsonpath_ng.ext import parse

    import re, json
    
    now = time.time()

    try:
        bs = BeautifulSoup(html_content, features="lxml")

        data = json.loads(bs.find_all(id="shoebox-uts-api-cache")[0].string)
      
    except Exception as e:
        logger.warning(f"Unable to extract data, document parsing with extruct failed with {type(e).__name__} - {str(e)}")
        return Restock()
  
    logger.trace(f"JSON loading done in {time.time() - now:.3f}s")

    # First phase, dead simple scanning of anything that looks useful
    value = Restock()
    if data:
        try:
            content_type_parse = parse("$..playables..contentType")
            price_parse = parse(f"$..playables..offers[?(@.kind=='buy' & (@.variant=='{'HD' if hd_price else 'SD'}'))].price")
            price_formatted_parse = parse(f"$..playables..offers[?(@.kind=='buy' & (@.variant=='{'HD' if hd_price else 'SD'}'))].priceFormatted")
            # pricecurrency_parse = parse(''.join(['$', product_filter, '..(pricecurrency|currency|priceCurrency)']))
            availability_parse = parse("$..playables..isItunes")
    
            content_type_result = content_type_parse.find(data)
            price_result = price_parse.find(data)
            price_formatted_result = price_formatted_parse.find(data)
            
            if price_result:             
                if len(price_result) > 1:
                    logger.warning(f"More than one price found {price_result}, throwing exception, cant use this plugin.")
                    raise MoreThanOnePriceFound()
                else:
                    value['price'] = price_result[0].value
    
            # pricecurrency_result = pricecurrency_parse.find(data)
            # if pricecurrency_result:
            #     value['currency'] = pricecurrency_result[0].value
            value['currency'] = ""
    
            availability_result = availability_parse.find(data)
            if availability_result and price_result:
                value['availability'] = availability_result[0].value
            else:
                value['availability'] = False
                
            logger.debug(value)
                
        except MoreThanOneResultReturned as e:
            raise e
        except Exception as e:
            logger.warning(f"Unable to extract data, document parsing with extruct failed with {type(e).__name__} - {str(e)}")
            return Restock()
            
    logger.trace(f"Processed with JSON in {time.time()-now:.3f}s")

    return value

# should return Restock()
# add casting?
def get_book_availability(html_content) -> Restock:
    """
    Kind of funny/cool way to find price/availability in one many different possibilities.
    Use 'extruct' to find any possible RDFa/microdata/json-ld data, make a JSON string from the output then search it.
    """
    from bs4 import BeautifulSoup
    
    import re, json
    
    now = time.time()

    try:
        bs = BeautifulSoup(html_content, features="html5lib")

        data = json.loads(bs.find_all("script", {"name": "schema:book"})[0].string)
      
    except Exception as e:
        logger.warning(f"Unable to extract data, document parsing with extruct failed with {type(e).__name__} - {str(e)}")
        return Restock()
        
    logger.debug(json.dumps(data, indent=2))

    logger.trace(f"JSON loading done in {time.time() - now:.3f}s")

    # First phase, dead simple scanning of anything that looks useful
    value = Restock()
    if data:
        try:
            # Check the result count, only 1 result should be returned if the product is available
            if data["@type"] != "Book":
                logger.info(f"No results returned")
                return Result()
            # elif data["offers"] > 1:
            #     logger.warning(f"More than one results returned")
            #     raise MoreThanOneResultReturned()
          
            offer = data["offers"]
            
            if offer:
                value['price'] = offer['price']
                value['currency'] = offer['priceCurrency']
                value['availability'] = True
            else:
                value["availability"] = False

        except MoreThanOneResultReturned as e:
            raise e
        except Exception as e:
            logger.warning(f"Unable to extract data, document parsing with extruct failed with {type(e).__name__} - {str(e)}")
            return Restock()
            
    logger.trace(f"Processed with JSON in {time.time()-now:.3f}s")

    return value

# should return Restock()
# add casting?
def get_itunes_availability(html_content, hd_price = False) -> Restock:
    """
    Kind of funny/cool way to find price/availability in one many different possibilities.
    Use 'extruct' to find any possible RDFa/microdata/json-ld data, make a JSON string from the output then search it.
    """
    from jsonpath_ng.ext import parse

    import re, json
    
    now = time.time()

    try:
        data = json.loads(html_content)
    except Exception as e:
        logger.warning(f"Unable to extract data, document parsing with extruct failed with {type(e).__name__} - {str(e)}")
        return Restock()
        
    logger.debug(json.dumps(data, indent=2))

    logger.trace(f"JSON loading done in {time.time() - now:.3f}s")

    # First phase, dead simple scanning of anything that looks useful
    value = Restock()
    if data:
        try:
            # Check the result count, only 1 result should be returned if the product is available
            if data["resultCount"] == 0:
                logger.info(f"No results returned")
                return Result()
            elif data["resultCount"] > 1:
                logger.warning(f"More than one results returned")
                raise MoreThanOneResultReturned()
            
            result = data['results'][0]
            
            # extract details depending on the type
            if 'wrapperType' in result:
                if result['wrapperType'] == "collection":
                    if result['collectionType'] == "TV Season" or result['collectionType'] == "Movie Bundle":
                        value['price'] = result['collectionHdPrice'] if hd_price and 'collectionHdPrice' in result else result['collectionPrice']
                        value['currency'] = result['currency']
                        value['availability'] = True
                elif result['wrapperType'] == "track":
                    if result['kind'] == "feature-movie":
                        value['price'] = result['trackHdPrice'] if hd_price and 'trackHdPrice' in result else result['trackPrice']
                        value['currency'] = result['currency']
                        value['availability'] = True
            elif 'kind' in result:
                if result['kind'] == "ebook":
                    value['price'] = result['price']
                    value['currency'] = result['currency']
                    value['availability'] = True

        except MoreThanOneResultReturned as e:
            raise e
        except Exception as e:
            logger.warning(f"Unable to extract data, document parsing with extruct failed with {type(e).__name__} - {str(e)}")
            return Restock()
            
    logger.trace(f"Processed with JSON in {time.time()-now:.3f}s")

    return value


def is_between(number, lower=None, upper=None):
    """
    Check if a number is between two values.

    Parameters:
    number (float): The number to check.
    lower (float or None): The lower bound (inclusive). If None, no lower bound.
    upper (float or None): The upper bound (inclusive). If None, no upper bound.

    Returns:
    bool: True if the number is between the lower and upper bounds, False otherwise.
    """
    return (lower is None or lower <= number) and (upper is None or number <= upper)


class perform_site_check(difference_detection_processor):
    screenshot = None
    xpath_data = None

    def run_changedetection(self, watch):
        import hashlib, json

        if not watch:
            raise Exception("Watch no longer exists.")

        # Unset any existing notification error
        update_obj = {'last_notification_error': False, 'last_error': False, 'restock':  Restock()}

        self.screenshot = self.fetcher.screenshot
        self.xpath_data = self.fetcher.xpath_data

        # Track the content type
        update_obj['content_type'] = self.fetcher.headers.get('Content-Type', '')
        update_obj["last_check_status"] = self.fetcher.get_last_status_code()
        
        # Copy the lowest_price from the watch
        update_obj["restock"]["lowest_price"] = watch["restock"]["lowest_price"] if watch["restock"]["lowest_price"] else None
        
        # Only try to process restock information (like scraping for keywords) if the page was actually rendered correctly.
        # Otherwise it will assume "in stock" because nothing suggesting the opposite was found
        from ...html_tools import html_to_text
        text = html_to_text(self.fetcher.content)
        logger.debug(f"Length of text after conversion: {len(text)}")
        if not len(text):
            from ...content_fetchers.exceptions import ReplyWithContentButNoText
            raise ReplyWithContentButNoText(url=watch.link,
                                            status_code=self.fetcher.get_last_status_code(),
                                            screenshot=self.fetcher.screenshot,
                                            html_content=self.fetcher.content,
                                            xpath_data=self.fetcher.xpath_data
                                            )
        
        # Which restock settings to compare against?
        itunes_settings = watch.get('itunes_settings', {})
        
        # See if any tags have 'activate for individual watches in this tag/group?' enabled and use the first we find
        for tag_uuid in watch.get('tags'):
            tag = self.datastore.data['settings']['application']['tags'].get(tag_uuid, {})
            if tag.get('overrides_watch'):
                itunes_settings = tag.get('itunes_settings', {})
                logger.info(f"Watch {watch.get('uuid')} - Tag '{tag.get('title')}' selected for restock settings override")
                break

        itemprop_availability = {}
        try:
            if watch.get("url").lower().startswith("https://books.apple.com/"):
              itemprop_availability = get_book_availability(self.fetcher.content)
            if watch.get("url").lower().startswith("https://itunes.apple.com/lookup"):
              itemprop_availability = get_itunes_availability(self.fetcher.content, itunes_settings.get("hd_price"))
            if watch.get("url").lower().startswith("https://tv.apple.com/"):
              itemprop_availability = get_appletv_availability(self.fetcher.content, itunes_settings.get("hd_price"))
        except MoreThanOnePriceFound as e:
            # Add the real data
            raise ProcessorException(message="Cannot run, more than one price detected, this plugin is only for product pages with ONE product, try the content-change detection mode.",
                                     url=watch.get('url'),
                                     status_code=self.fetcher.get_last_status_code(),
                                     screenshot=self.fetcher.screenshot,
                                     xpath_data=self.fetcher.xpath_data
                                     )
        except MoreThanOneResultReturned as e:
            # Add the real data
            raise ProcessorException(message="Cannot run, more than one result returned, this plugin is expecting only ONE result be returned by the URL.",
                                     url=watch.get('url'),
                                     status_code=self.fetcher.get_last_status_code(),
                                     screenshot=self.fetcher.screenshot,
                                     xpath_data=self.fetcher.xpath_data
                                     )
                                     
        # Something valid in get_itemprop_availability() by scraping metadata ?
        if itemprop_availability.get('price') or itemprop_availability.get('availability') == True:
            # Store for other usage
            update_obj['restock'] = itemprop_availability
            update_obj['restock']['in_stock'] = True
        else:
            update_obj['restock']['in_stock'] = False
            
        # Copy the lowest_price from the watch
        update_obj["restock"]["lowest_price"] = watch["restock"]["lowest_price"] if watch["restock"]["lowest_price"] else None

        # Main detection method
        fetched_md5 = None
        
        # Update the original and/or lowest price if a price is available
        if itemprop_availability and itemprop_availability.get('price'):
          # store original price if not set
            if not itemprop_availability.get('original_price'):
                itemprop_availability['original_price'] = itemprop_availability.get('price')
                update_obj['restock']["original_price"] = itemprop_availability.get('price')
            
            # update the lowest price if not set or new price is lower
            if not watch['restock']['lowest_price'] or float(itemprop_availability.get('price')) < watch['restock']['lowest_price']:
                logger.debug(f"Watch UUID {watch.get('uuid')} new lowest price {itemprop_availability.get('price')}")
                update_obj['restock']['lowest_price'] = itemprop_availability.get('price')
                
        if not self.fetcher.instock_data and not itemprop_availability.get('availability') and not itemprop_availability.get('price'):
            raise ProcessorException(
                message=f"Unable to extract restock data for this page unfortunately. (Got code {self.fetcher.get_last_status_code()} from server), no embedded stock information was found and nothing interesting in the text, try using this watch with Chrome.",
                url=watch.get('url'),
                status_code=self.fetcher.get_last_status_code(),
                screenshot=self.fetcher.screenshot,
                xpath_data=self.fetcher.xpath_data
                )

        logger.debug(f"self.fetcher.instock_data is - '{self.fetcher.instock_data}' and itemprop_availability.get('availability') is {itemprop_availability.get('availability')}")
        # Nothing automatic in microdata found, revert to scraping the page
        if self.fetcher.instock_data and itemprop_availability.get('availability') is None:
            # 'Possibly in stock' comes from stock-not-in-stock.js when no string found above the fold.
            # Careful! this does not really come from chrome/js when the watch is set to plaintext
            update_obj['restock']["in_stock"] = True if self.fetcher.instock_data == 'Possibly in stock' else False
            logger.debug(f"Watch UUID {watch.get('uuid')} restock check returned instock_data - '{self.fetcher.instock_data}' from JS scraper.")

        # Very often websites will lie about the 'availability' in the metadata, so if the scraped version says its NOT in stock, use that.
        if self.fetcher.instock_data and self.fetcher.instock_data != 'Possibly in stock':
            if update_obj['restock'].get('in_stock'):
                logger.warning(
                    f"Lie detected in the availability machine data!! when scraping said its not in stock!! itemprop was '{itemprop_availability}' and scraped from browser was '{self.fetcher.instock_data}' update obj was {update_obj['restock']} ")
                logger.warning(f"Setting instock to FALSE, scraper found '{self.fetcher.instock_data}' in the body but metadata reported not-in-stock")
                update_obj['restock']["in_stock"] = False

        # What we store in the snapshot
        price = update_obj.get('restock').get('price') if update_obj.get('restock').get('price') else ""
        snapshot_content = f"In Stock: {update_obj.get('restock').get('in_stock')} - Price: {price}"

        # Main detection method
        fetched_md5 = hashlib.md5(snapshot_content.encode('utf-8')).hexdigest()

        # The main thing that all this at the moment comes down to :)
        changed_detected = False
        logger.debug(f"Watch UUID {watch.get('uuid')} restock check - Previous MD5: {watch.get('previous_md5')}, Fetched MD5 {fetched_md5}")

        # out of stock -> back in stock only?
        if watch.get('restock') and watch['restock'].get('in_stock') != update_obj['restock'].get('in_stock'):
            # Yes if we only care about it going to instock, AND we are in stock
            if itunes_settings.get('in_stock_processing') == 'in_stock_only' and update_obj['restock']['in_stock']:
                changed_detected = True

            if itunes_settings.get('in_stock_processing') == 'all_changes':
                # All cases
                changed_detected = True

        if itunes_settings.get('follow_price_changes') and watch.get('restock') and update_obj.get('restock') and update_obj['restock'].get('price'):
            price = float(update_obj['restock'].get('price'))
            # Default to current price if no previous price found
            if watch['restock'].get('original_price'):
                previous_price = float(watch['restock'].get('original_price'))
                # It was different, but negate it further down
                if price != previous_price:
                    changed_detected = True

            # Minimum/maximum price limit
            if update_obj.get('restock') and update_obj['restock'].get('price'):
                logger.debug(
                    f"{watch.get('uuid')} - Price was detected, 'price_change_max' is '{itunes_settings.get('price_change_max', '')}' 'price_change_min' is '{itunes_settings.get('price_change_min', '')}', 'lowest_price' is '{watch['restock'].get('lowest_price', '')}', price from website is '{update_obj['restock'].get('price', '')}'.")
                if update_obj['restock'].get('price'):
                    lowest_price = float(watch['restock'].get('lowest_price')) if watch['restock'].get('lowest_price') else None
                    min_limit = float(itunes_settings.get('price_change_min')) if itunes_settings.get('price_change_min') else None
                    max_limit = float(itunes_settings.get('price_change_max')) if itunes_settings.get('price_change_max') else None

                    price = float(update_obj['restock'].get('price'))
                    logger.debug(f"{watch.get('uuid')} after float conversion - Min limit: '{min_limit}' Max limit: '{max_limit}' Price: '{price}'")
                    if min_limit or max_limit:
                        if is_between(number=price, lower=min_limit, upper=max_limit):
                            # Price was between min/max limit, so there was nothing todo in any case
                            logger.trace(f"{watch.get('uuid')} {price} is between {min_limit} and {max_limit}, nothing to check, forcing changed_detected = False (was {changed_detected})")
                            changed_detected = False
                        else:
                            logger.trace(f"{watch.get('uuid')} {price} is between {min_limit} and {max_limit}, continuing normal comparison")

                    # Price comparison by %
                    if watch['restock'].get('original_price') and changed_detected and itunes_settings.get('price_change_threshold_percent'):
                        previous_price = float(watch['restock'].get('original_price'))
                        pc = float(itunes_settings.get('price_change_threshold_percent'))
                        change = abs((price - previous_price) / previous_price * 100)
                        if change and change <= pc:
                            logger.debug(f"{watch.get('uuid')} Override change-detected to FALSE because % threshold ({pc}%) was {change:.3f}%")
                            changed_detected = False
                        else:
                            logger.debug(f"{watch.get('uuid')} Price change was {change:.3f}% , (threshold {pc}%)")

                    # Check if lowest price
                    if itunes_settings.get('lowest_price_only') and changed_detected and lowest_price and price > lowest_price:
                        logger.debug(f"{watch.get('uuid')} Override change-detected to FALSE because price ({price}) was higher than lowest price ({lowest_price})")
                        changed_detected = False
                            
        # Always record the new checksum
        update_obj["previous_md5"] = fetched_md5

        logger.debug(json.dumps(update_obj, indent=2))
        
        return changed_detected, update_obj, snapshot_content.strip()

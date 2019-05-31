# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 22:44:09 2019
@author: Serdarcan Dilbaz
"""

# Web related modules
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
#from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import WebDriverException
#from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
from urllib.request import urlopen
import html2text

# YouTube download module
from pytube import YouTube

# Multiprocessing tools
from multiprocessing import Lock, Manager, Pool, Process
import multiprocessing as mp

# Misc modules
import time, re, pickle, os, shutil, argparse, glob, unicodedata, datetime
from argparse import RawTextHelpFormatter

# Contractions dictionary for expanding contractions in the advertisement website source text
from contractions import CONTRACTION_MAP


def download_vids(vid_queue,save_loc,max_length):
    while not vid_queue.qsize():
        vid_id=vid_queue.get()
        print(vid_id)
        dest=os.path.join(save_loc,vid_id)
        if not os.path.isdir(dest):
            os.mkdir(dest)
        if not glob.glob(os.path.join(dest,'*.mp4')):
            yt=YouTube('https://www.youtube.com/watch?v='+vid_id)
            if int(yt.length)<max_length:
                yt.streams.first().download(dest)

def add2q(vid_queue,vid_ids,save_loc):
    if type(vid_ids)==str:
        dest=os.path.join(save_loc,vid_ids)
        if not os.path.isdir(dest):
            os.mkdir(dest)
        if not glob.glob(os.path.join(dest,'*.mp4')):
            vid_queue.put(vid_ids)
        
    elif type(vid_ids)==list:
        for vid_id in vid_ids:
            add2q(vid_queue,vid_id,save_loc)
    else:
        raise TypeError('Wrong input format for saving Vids, expected str or list')
    
def remove_accented_chars(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return text

def expand_contractions(text, contraction_mapping=CONTRACTION_MAP):
    
    contractions_pattern = re.compile('({})'.format('|'.join(contraction_mapping.keys())), 
                                      flags=re.IGNORECASE|re.DOTALL)
    def expand_match(contraction):
        match = contraction.group(0)
        first_char = match[0]
        expanded_contraction = contraction_mapping.get(match)\
                                if contraction_mapping.get(match)\
                                else contraction_mapping.get(match.lower())                       
        expanded_contraction = first_char+expanded_contraction[1:]
        return expanded_contraction
        
    expanded_text = contractions_pattern.sub(expand_match, text)
    expanded_text = re.sub("'", "", expanded_text)
    return expanded_text

def remove_special_characters(text, remove_digits=False):
    pattern = r'[^a-zA-z0-9\s]' if not remove_digits else r'[^a-zA-z\s]'
    text = re.sub(pattern, '', text)
    return text

def normalize_corpus(corpus, contraction_expansion=True,
                     accented_char_removal=True, text_lower_case=False,
                     special_char_removal=False, vs=10000, dim=300):
    normalized_corpus = []
    # normalize each document in the corpus
    for doc in corpus.split(" "):
        # remove accented characters
        if accented_char_removal:
            doc = remove_accented_chars(doc)
        # expand contractions    
        if contraction_expansion:
            doc = expand_contractions(doc)
        # lowercase the text    
        if text_lower_case:
            doc = doc.lower()
            
        normalized_corpus.append(doc)
    
    return ' '.join(normalized_corpus)



def explore_home(chromedriver_path,chrome_options,caps):
    scroll_sleep=1
    scroll_count=20
    driver=webdriver.Chrome(executable_path=chromedriver_path,options=chrome_options,desired_capabilities=caps)
    driver.get('https://www.youtube.com')
    time.sleep(scroll_sleep)
    for scroll in range(scroll_count):
        driver.execute_script("window.scrollTo(0, "+str(scroll*driver.get_window_size()['height'])+");" )
        time.sleep(scroll_sleep)
        
    browser_log = driver.get_log('performance')
    vids = [item[:11] for item in str(browser_log).split('https://i.ytimg.com/vi/')]
    return [vid for vid in vids if len(re.findall(r'[0-9]|[a-z]|[A-Z]|_|-',vid))==11 and len(vid)==11]

def explore_vid(chromedriver_path,chrome_options,caps,vid,ads,save_loc,max_length,vid_queue):
    driver=webdriver.Chrome(executable_path=chromedriver_path,options=chrome_options,desired_capabilities=caps)
#    driver.implicitly_wait(60)
    driver.get('https://www.youtube.com/watch?v='+vid)
    time.sleep(2)
    
    sec_html = driver.page_source
    soup=BeautifulSoup(sec_html,'lxml')
    mydivs = str(soup.findAll("div", {"class": "style-scope ytd-watch-next-secondary-results-renderer"}))
    inds=[m.start() for m in re.finditer('ytimg.com/vi/', mydivs)]
    rec_vids=['https://www.youtube.com/watch?v='+mydivs[ind+13:ind+24] for ind in inds]
    
    browser_log = driver.get_log('performance') 
    adInfo=find_ad(browser_log,vid)
    
    if adInfo:
        #Check if it is the first time this ad has been seen
        adID=adInfo[0]
        
        if adID in ads:
            driver.quit()
            times=ads[adID][0]
            ad_website_URL=ads[adID][1]
            ads.pop(adID)
            ads[adID]=[times+[adInfo[1]],ad_website_URL]
            #ads[adID][0].append(adInfo[1])
        else:
            try:
                element=WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[starts-with(@id, 'visit-advertiser:')]")))
                element.click()
            except:
                try:
                    element = driver.find_element_by_css_selector(".ytp-ad-button.ytp-ad-visit-advertiser-button.ytp-ad-button-link")
                    element.click()
                except:
                    try:
                        element = driver.find_element_by_tag_name("button")
                        element.click()  
                    except:
                        try:
                            element = driver.find_element_by_css_selector(".ytp-ad-button-text")
                            element.click()
                        except:
                            try:
                                element = driver.find_element_by_css_selector(".ytp-ad-button.ytp-ad-button-link.ytp-ad-clickable")
                                element.click()
                            except:
                                print('Button click failed.\nOriginal ID: %s Ad ID: %s' %(vid,adInfo[0]))
                                button_locator(driver)
                                time.sleep(1000)

            if len(driver.window_handles)>1:
                driver.switch_to.window(driver.window_handles[-1])
                num_trials=10
                for trial in range(num_trials):
                    try:
                        ad_website_URL=driver.current_url
                        continue
                    except WebDriverException:
                        time.sleep(5)
                ads[adID]=[[adInfo[1]],ad_website_URL]
                ad_website_HTML=driver.page_source
                
                if not os.path.isdir(os.path.join(save_loc,adID)):
                    os.mkdir(os.path.join(save_loc,adID))
                clean_text=html2text.html2text(ad_website_HTML)
                clean_text=normalize_corpus(re.sub('\s+', ' ', clean_text).strip())
                
                textName=os.path.join(save_loc,adID,'adwebsite.txt')
                
                file = open(textName,"w") 
     
                file.write(ad_website_URL)
                file.write('\n')
                file.write(clean_text)
             
                file.close() 
                
            driver.quit()
            add2q(vid_queue,adID,save_loc)
            
    else:
        driver.quit()
    return rec_vids

def button_locator(driver):
    driver.find_element_by_tag_name('body').send_keys(Keys.SPACE)
    el=driver.switch_to_active_element()
    print(el.attribute)
    print('Att:')
    attrs = driver.execute_script('var items = {}; for (index = 0; index < arguments[0].attributes.length; ++index) { items[arguments[0].attributes[index].name] = arguments[0].attributes[index].value }; return items;', el)
    print(attrs)
    for k in range(20):
        driver.find_element_by_tag_name('body').send_keys(Keys.TAB)
        el=driver.switch_to_active_element()
        print(el.text)
        print('Att:')
        attrs = driver.execute_script('var items = {}; for (index = 0; index < arguments[0].attributes.length; ++index) { items[arguments[0].attributes[index].name] = arguments[0].attributes[index].value }; return items;', el)
        print(attrs)
        
def find_ad(browser_log,vid):
    for k in range(len(browser_log)):
        if browser_log[k]['message'].find('adunit')!=-1 and browser_log[k]['message'].find(vid)!=-1:
            ind=browser_log[k]['message'].find('https://www.youtube.com/get_video_info?html5=1&video_id=')
            vid_id=browser_log[k]['message'][ind+56:ind+67]
            return (vid_id,time.localtime())
    return None

def positive_int(argument):
    num=int(argument)
    if num<1:
        msg="Maximum depth parameter must be a positive number. You entered: %s" %argument
        raise argparse.ArgumentTypeError(msg)
    return num

def valid_pickle(argument):
    file=str(argument)
    if not file.endswith('.pickle'):
        msg="ad_save_loc must end with .pickle You entered: %s" %file
        raise argparse.ArgumentTypeError(msg)
    return file

def valid_dir(argument):
    directory=str(argument)
    if not os.path.isdir(directory):
        msg="vid_save_loc must be a valid directory. You entered: %s" %directory
        raise argparse.ArgumentTypeError(msg)
    return directory

if __name__ == '__main__':
    # Argument Parsing
    parser = argparse.ArgumentParser(description='Scrapes Youtube ads and advertising company websites. \nUse --restart to restart the scraping from scratch by deleting previous data\nExample Usage: python finalReader.py E:\ads\ads.pickle E:\ads --ncpu 2', formatter_class=RawTextHelpFormatter)
    parser.add_argument('ad_save_loc',help='Save Location for Ad Main Dictionary', type=valid_pickle)
    parser.add_argument('vid_save_loc',help='Save Location for Ad Videos', type=valid_dir)
    parser.add_argument('chromedriver_path', help='Path of the chrome executable', type=str)
    parser.add_argument('--restart', help='Restart collection', action="store_true", default=False, dest='restartCollection')
    parser.add_argument('--ncpu', nargs='?', help='Number of cores for multiprocessing, 1 by default', default=1, type=int, dest='mpcpu')
    parser.add_argument('--timeout',nargs='?', help='For how long the data collection will take place (in seconds), infinite by default', default=float('inf'), type=float, dest='time_limit')
    parser.add_argument('--max_depth', nargs='?', help='Depth of Youtube exploration tree', default=1, type=positive_int, dest='search_depth')
    parser.add_argument('--max_ad_length', nargs='?', help='Maximum allowed length of Youtube advertisement videos in seconds', default=600, type=positive_int, dest='max_length')
    parser.add_argument('--saving_interval', nargs='?', help='Saving interval for the dictionary of advertisements', default=10, type=positive_int, dest='saving_interval')
    args = parser.parse_args()

    ad_save_loc=args.ad_save_loc
    vid_save_loc=args.vid_save_loc
    vid_save_loc=os.path.join(vid_save_loc,'ad_data')
    mpcpu=max(args.mpcpu,1)
    time_limit=args.time_limit
    chromedriver_path=args.chromedriver_path
    search_depth=args.search_depth
    max_length=args.max_length
    saving_interval=args.saving_interval
    
    if not os.path.isdir(vid_save_loc):
        os.mkdir(vid_save_loc)

    if args.restartCollection:
        for the_file in os.listdir(vid_save_loc):
            file_path = os.path.join(vid_save_loc, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(e)

        if os.path.isfile(ad_save_loc):
            os.remove(ad_save_loc)
        ads={}
    else:
        if os.path.isfile(ad_save_loc):
            pickle_in = open(ad_save_loc,"rb")
            ads = pickle.load(pickle_in)
        else:
            ads={}
    
    manager=Manager()
    ads=manager.dict(ads)
    vid2load=manager.Queue()
    pool = Pool(processes=mpcpu)
    
#    Chrome Driver Options
    chrome_options=Options()
    chrome_options.add_argument('--mute-audio')
    caps = DesiredCapabilities.CHROME
    caps['loggingPrefs'] = {'performance': 'ALL'}
    
    startTime=time.time()
    currentTime=time.time()
    
    while currentTime-startTime<time_limit:
        print('Time from start: %s' %str(datetime.timedelta(seconds=currentTime-startTime)))
        rec_vids=explore_home(chromedriver_path,chrome_options,caps)
        while not rec_vids:
            time.sleep(60)
            rec_vids=explore_home(chromedriver_path,chrome_options,caps)
        
        for depth in range(search_depth):
            print('Depth %s' %depth)
            multiple_results=[pool.apply_async(explore_vid, (chromedriver_path,chrome_options,caps,vid,ads,vid_save_loc,max_length,vid2load)) for vid in rec_vids]
            branching_vids=[]
            
            for ind,res in enumerate(multiple_results):        
                branching_vids.append(res.get())
                if not ind%saving_interval:
                    print('saving')
                    pickle_out = open(ad_save_loc,"wb")
                    pickle.dump(dict(ads), pickle_out)
                    pickle_out.close()
                    download_vids(vid2load,vid_save_loc,max_length)
                    
                if time.time()-startTime<time_limit:
                    break
            res_vids=branching_vids.copy()
        

        currentTime=time.time()

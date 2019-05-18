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
from multiprocessing import Lock, Manager, Queue, Pool
import multiprocessing as mp

# Misc modules
import time, re, pickle, os, shutil, argparse, glob, unicodedata, datetime
from argparse import RawTextHelpFormatter

# Contractions dictionary for expanding contractions in the advertisement website source text
from contractions import CONTRACTION_MAP

def save_vids(vid_ids,save_loc):
    if type(vid_ids)==str:
        dest=os.path.join(save_loc,vid_ids)
        if not os.path.isdir(dest):
            os.mkdir(dest)
        if not glob.glob(os.path.join(dest,'*.mp4')):
            yt=YouTube('https://www.youtube.com/watch?v='+vid_ids)
            yt.streams.first().download(dest)
        
    elif type(vid_ids)==list:
        for vid_id in vid_ids:
            save_vids(vid_id,save_loc)
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
    driver=webdriver.Chrome(executable_path=chromedriver_path,options=chrome_options,desired_capabilities=caps)
    driver.get('https://www.youtube.com')
    time.sleep(1)
    html_source = driver.page_source


    driver.close()
    parts=html_source.split('{"webCommandMetadata":{"url":"/watch_videos?')[1:]
    vids=[]
    for part in parts:
        part=part[part.find('video_ids=')+10:]
        
        if part.find('\\u')!=-1:
            if part.find('"')!=-1:
                end=min(part.find('\\u'),part.find('"'))
            else:
                end=part.find('\\u')
        elif part.find('"')!=-1:
            end=part.find('"')
        else:
            print('No video found on YouTube homepage')
        concat_list=part[:end]
        vids.extend(concat_list.split('%2C'))
    vids=[vid for vid in vids if len(re.findall(r'[0-9]|[a-z]|[A-Z]|_|-',vid))==11 and len(vid)==11]

    return vids

def explore_vid(chromedriver_path,chrome_options,caps,vid,ads,save_loc):
    print(ads)
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
            times=ads[adID][0]
            ad_website_URL=ads[adID][1]
            ads.pop(adID)
            ads[adID]=[times+[adInfo[1]],ad_website_URL]
            #ads[adID][0].append(adInfo[1])
        else:
            # Fullscreen
#                driver.find_element_by_tag_name('body').send_keys("f")
            try:
                element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "button")))
                element.click()
            except:
                try:
                    element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".ytp-ad-button.ytp-ad-visit-advertiser-button.ytp-ad-button-link")))
                    element.click()
                            
                except WebDriverException:
                    print('Button click failed: %s:%s' %(vid,adInfo[0]))

            if len(driver.window_handles)!=1:
                driver.switch_to.window(driver.window_handles[-1])
                ad_website_URL=driver.current_url
                ads[adID]=[[adInfo[1]],ad_website_URL]
                ad_website_HTML=driver.page_source
                clean_text=html2text.html2text(ad_website_HTML)
                clean_text=normalize_corpus(re.sub('\s+', ' ', clean_text).strip())
                
                save_vids(adID,save_loc)
                
                textName=os.path.join(save_loc,adID,'adwebsite.txt')
    
    
                file = open(textName,"w") 
     
                file.write(ad_website_URL)
                file.write('\n')
                file.write(clean_text)
                 
                file.close() 
            
            

    driver.quit()
    return rec_vids
    

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
    args = parser.parse_args()

    ad_save_loc=args.ad_save_loc
    vid_save_loc=args.vid_save_loc
    vid_save_loc=os.path.join(vid_save_loc,'ad_data')
    mpcpu=max(args.mpcpu,1)
    time_limit=args.time_limit
    chromedriver_path=args.chromedriver_path
    search_depth=args.search_depth

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
    
#    Chrome Driver Options
    chrome_options=Options()
#    chrome_options.headless=True
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
        
        m = Manager()
                
        pool = Pool(processes=mpcpu)
        
        for depth in range(search_depth):
            print('Depth %s' %depth)
            multiple_results=[pool.apply_async(explore_vid, (chromedriver_path,chrome_options,caps,vid,ads,vid_save_loc)) for vid in rec_vids]
            branching_vids=[]
            
            for res in multiple_results:        
                branching_vids.append(res.get())
                if time.time()-startTime<time_limit:
                    break
            res_vids=branching_vids.copy()
        
            pickle_out = open(ad_save_loc,"wb")
            pickle.dump(dict(ads), pickle_out)
            pickle_out.close()

        currentTime=time.time()

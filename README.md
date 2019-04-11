# Youtube-Advertisement-Collector
This code is for identifying advertisement videos that run before YouTube videos  and collecting corresponding information. The advertisement video, the website url the advertisement points to, the page source of the advertising company, and the time the advertisement is encountered is stored. The code utilizes the current structure and layout of YouTube, so the scraper may not work if the YouTube layout changes.

For speeding up the process, the scraping is carried out using multiprocessing, so when running the code as is, run from command line.

The Youtube homepage is visited using Selenium Chrome webdriver. The list of trending videos as well as videos from topics such as sports, gaming etc. are extracted. The topics besides the trending are selected by YouTube. The video exploration is carried out in a similar manner to breadth-first search. The maximum depth exploration is specified in order to avoid overspecification in the recommended videos. Depending on the exploration depth, each video is visited and the recommended videos for the video are queued. Since the advertisement is dependent on the video, the goal is to collect a representative subset of the advertisements by branching out from the homepage of YouTube.

Whenever an advertisement is encountered, the browser log is used to extract the advertisement video id. The advertisement video id is used to log the time the ad was encountered and if the advertisement is encountered for the first time, the video is downloaded with PyTube. In addition, if the advertisement is encountered for the first time, the website the advertisement points to is logged and the source code of the webpage is extracted. The source code is cleaned so that the text consists of letters and numbers. The html2text library is used to convert the pagesource in a format ready for natural language processing.

Example usage:
	python finalReader.py ads.pickle test "...\chromedriver.exe" --ncpu 1 --restart --timeout 60 --max_depth 2

Functions:
	
	combineDicts(l, ads, currentDic)
Combines the master dictionary of advertisements and the current dictionary which is generated from a branch of YouTube exploration.
	
	saveVids(vid_ids, saveLoc)
Downloads the list of video ids or a video id of type str associated with the ads to the saveLoc

	remove_accented_chars(text)
Removes Accented characters from text
	
	expand_contractions(text, contraction_mapping=CONTRACTION_MAP)
expands contractions in the text for special character removal
	
	remove_special_characters(text, remove_digits=False)
All characters that aren't letters or numbers are removed from string
	
	normalize_corpus(...)
Given a string, normalizes the string by applying the previous functions
	
	readCleanAdSite(url)
Used for extracting clean text from the advertisement website. Visits website, cleans html
	
	explore_home(chromedriver_path,chrome_options,caps)
Returns trending video urls from the YouTube homepage
	
	explore_vid(chromedriver_path,chrome_options,caps,vid,ads,save_loc,l)
Given a video id, visits page, checks for an ad, if there is a new ad downloads the ad video and saves the contents of the advertising company's website contents. If the ad has been encountered before, the time is recorded.


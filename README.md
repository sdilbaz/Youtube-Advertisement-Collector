# Youtube-Advertisement-Collector
This code is for identifying advertisement videos that run before YouTube videos  and collecting corresponding information. The advertisement video, the website url the advertisement points to, the page source of the advertising company, and the time the advertisement is encountered is stored. The code utilizes the current structure and layout of YouTube, so the scraper may not work if the YouTube layout changes.

For speeding up the process, the scraping is carried out using multiprocessing, so when running the code as is, run from command line.

The Youtube homepage is visited using Selenium Chrome webdriver. The chrome driver uses the chromedriver executable which can be downloaded [here](http://chromedriver.chromium.org/downloads). This code is tested with Chrome version 72. The list of trending videos as well as videos from topics such as sports, gaming etc. are extracted. The topics besides the trending are selected by YouTube. The video exploration is carried out in a similar manner to breadth-first search. The maximum depth exploration is specified in order to avoid overspecification in the recommended videos. Depending on the exploration depth, each video is visited and the recommended videos for the video are queued. Since the advertisement is dependent on the video, the goal is to collect a representative subset of the advertisements by branching out from the homepage of YouTube.

The YouTube Ad collection consists of the following steps:
1. Youtube homepage is visited and an initial list of YouTube videos are collected.
2. The Chrome driver visits each YouTube video and checks if a advertisement is played before the video by analyzing the browser log.
3. If there is an ad, the YouTube video id of the advertisement is extracted.
4. The video id of the ad is used to download the advertisement video with PyTube if the advertisement has been encountered for the first time.
5. If there is a clickable link for visiting the advertising company, the link is clicked to navigate to the associated website.
6. The pagesource of the advertisement company is extracted, cleaned, and stored.
7. Recommended videos are stored and the process is repeated until the maximum exploration depth is reach or the time limit for data collection is reached.

Example usage:
	
	python ad_collector.py ads.pickle video_save_dir "...\chromedriver.exe" --ncpu 1 --restart --timeout 60 --max_depth 2

The ad information is stored as a dictionary in ads.pickle. The ad videos are stored in video_save_dir where a new directory for each ad is created. The cleaned page source of the ad company is also stored in the same directory as a text file. "...\chromedriver.exe" is the path to the [chrome driver](http://chromedriver.chromium.org/downloads).

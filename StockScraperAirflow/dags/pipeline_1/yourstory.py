from pipeline_1.interface_news import INews
from selenium import webdriver
import time
from selenium.webdriver.common.by import By


class YourStory(INews):
    "Yourstory concrete class implements the INews interface"

    def __init__(self):
        self.keywords = ["Hdfc", "Tata Motors"]
        self.bloglinks = {}
        self.blogtexts = {}

    def fetch_data(self):
        for keyword in self.keywords:
            # driver = webdriver.Chrome()
            options = webdriver.ChromeOptions()
            driver = webdriver.Remote("http://172.17.0.2:4444/wd/hub", options=options)
            driver.get("https://yourstory.com/search")
            time.sleep(10)
            search_box = driver.find_element(By.XPATH, "//*[@id='q']")
            search_box.send_keys(keyword)
            time.sleep(5)
            top_five_links = []
            for i in range(1,6):
                li = driver.find_element(By.XPATH, "//*[@id='header-collapse-trigger']/main/section/div[2]/div/div[{}]/div/li/a".format(i))
                top_five_links.append(li.get_attribute("href"))
            self.bloglinks[keyword] = top_five_links
            driver.quit()

        print("links: {}".format(self.bloglinks))

        print("starting fetching blog texts for yourstory")

        for keyword in self.keywords:
            top_five_links = self.bloglinks[keyword]
            blog_text_all = []
            for link in top_five_links:
                # driver = webdriver.Chrome()
                options = webdriver.ChromeOptions()
                driver = webdriver.Remote("http://172.17.0.2:4444/wd/hub", options=options)
                driver.get(link)
                time.sleep(8)
                title_h1_tag_elements = driver.find_elements(By.TAG_NAME, "h1")
                for element in title_h1_tag_elements:
                    if element.text.lower().strip() != "recommended stories":
                        title = element.text
                content = driver.find_element(By.XPATH, "//*[@id='article_container']/div[1]")
                blog_text_all.append({title: content.text})
                driver.quit()
            self.blogtexts[keyword] = blog_text_all 

        print("finished fetching blog texts for yourstory")

        return self.blogtexts
    


    


from pipeline_1.interface_news import INews
from selenium import webdriver
import time
from selenium.webdriver.common.by import By

class FinShots(INews):
    "finshots concrete class implements the INews interface"

    def __init__(self):
        self.keywords = ["Hdfc", "Tata Motors"]
        self.bloglinks = {}
        self.blogtexts = {}

    def fetch_data(self):
        for keyword in self.keywords:
            # driver = webdriver.Chrome()
            options = webdriver.ChromeOptions()
            driver = webdriver.Remote("http://172.17.0.2:4444/wd/hub", options=options)
            driver.get("https://finshots.in")
            time.sleep(10)
            search_button = driver.find_element(By.XPATH, "/html/body/div[4]/header/div/div/div[2]/div[2]/ul/li/a/div")
            search_button.click()
            input = driver.find_element(By.XPATH, "/html/body/div[3]/div/div[1]/div/form/input")
            input.send_keys(keyword)
            time.sleep(5)
            top_five_links = []
            for i in range(1,6):
                li = driver.find_element(By.XPATH, "/html/body/div[3]/div/div[2]/div/div/a[{}]".format(i))
                top_five_links.append(li.get_attribute("href"))
            self.bloglinks[keyword] = top_five_links
            driver.quit()	

        print("links: {}".format(self.bloglinks))

        print("starting fetching blog texts for finshots")

        for keyword in self.keywords:
            top_five_links = self.bloglinks[keyword]
            blog_text_all = []
            for link in top_five_links:
                # driver = webdriver.Chrome()
                options = webdriver.ChromeOptions()
                driver = webdriver.Remote("http://172.17.0.2:4444/wd/hub", options=options)
                driver.get(link)
                time.sleep(8)
                title = driver.find_element(By.XPATH,"//*[@id='site-main']/div/article/header/h1")
                content = driver.find_element(By.XPATH, "//*[@id='site-main']/div/article/section/div")
                blog_text_all.append({title.text : content.text})
                driver.quit()
            self.blogtexts[keyword] = blog_text_all

        print("finished fetching blog texts for finshots")

        return self.blogtexts



    


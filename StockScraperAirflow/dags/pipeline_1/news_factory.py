from pipeline_1.finshots import FinShots
from pipeline_1.yourstory import YourStory

class NewsFactory:  # pylint: disable=too-few-public-methods
    "The Factory Class"

    @staticmethod
    def get_news_source(news):
        "A static method to get a chair"
        if news.lower() == 'finshots':
            return FinShots()
        if news.lower() == 'yourstory':
            return YourStory()
        return None

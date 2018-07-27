import requests
import re
from bs4 import BeautifulSoup
from kafka import KafkaConsumer

def get_details(link):
    f = open("recipe_details.txt","w")
    try:
        r = requests.get(link)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "lxml")
            ingredients = soup.find_all('li', attrs={'itemprop':'ingredients'})
            regex = r"[^<li itemprop=\"ingredients\"](?P<ingredient>[^<]+)[<\/]"
            for ing in ingredients:
                test_str = str(ing)
                matches = re.finditer(regex, test_str, re.MULTILINE)
                for matchNum, match in enumerate(matches):
                    matchNum = matchNum + 1
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                        f.write(match.group(groupNum) + "\n")
            print("Details are saved in a file.")
    except:
        print("Couldn't get details")
    finally:
        f.close()

def what_to_cook():
    all_recipes = [] # if you run the kafka_producer.py several times, there will be more than 1 recipe in the topic
                     # and python will read from beginning (as --from-beginning command refers)
                     # to get the latest message(daily special) i defined this variable

    consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest', consumer_timeout_ms=1000, enable_auto_commit=False)
    consumer.subscribe(['recipe'])
    for m in consumer:
        all_recipes.append(m.value)
    print("Daily special: " + all_recipes[-1]) #print the latest msg
    get_details(all_recipes[-1])

if __name__ == "__main__":
    what_to_cook()

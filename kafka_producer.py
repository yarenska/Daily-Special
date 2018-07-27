# Before running this code, start both Zookeper and Kafka server respectively
# bin/zookeeper-server-start.sh config/zookeeper.properties
# bin/kafka-server-start.sh config/server.properties

import subprocess
import os
import requests
import random
import time
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from kafka.producer import kafka


def get_recipes():
    recipes = []
    try:
        r = requests.get("https://www.nefisyemektarifleri.com/")
        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html, "lxml")
            https_links = soup.select('.tarif-img-div a')
            for link in https_links:
                recipe_link = link['href']
                recipes.append(recipe_link)

    except:
        print("ERROR! Couldn't get recipes..")
    return recipes[random.randint(1,10)]

def send_message(daily_special):
    topic = "recipe"
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        producer.send(topic, bytes(daily_special))
        time.sleep(2)
        print("Daily special is sent..")
        producer.close()
    except Exception as e:
        print(str(e))

def create_topic():
    path = raw_input("Enter the path of your kafka file: ")
    try:
        os.chdir(path)
        print(os.getcwd())

        # Now create a new topic named recipe
        # ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic recipe
        try:
            subprocess.call(["./bin/kafka-topics.sh", "--create", "--zookeeper", "localhost:2181", "--replication-factor", "1", "--partitions", "1", "--topic", "recipe"])
        except kafka.common.TopicExistsException as e:
            e.message
            pass

    except Exception as e:
        e.message

if __name__ == "__main__":
    create_topic()
    daily_special = get_recipes()
    print(daily_special)
    send_message(daily_special)

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector as mysql
import json
import os

class OttPlatformPipeline:
    def __init__(self):
        """ creating connection with relational DB """
        self.create_connection()

    # config file 
    with open("OTT_platform\\config.json","r") as config:
        configuration = json.load(config)

    def process_item(self, item, spider):
        self.insert_record(item)
        return item

    def insert_record(self,record): 
        """ each item insertion into DB """
        query = "INSERT INTO records(title,site_name,description,release_year,genre,cast,imdb_rating,duration,image,platform) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (record.get("Title"),record.get("site name"),record.get("Description"),record.get("Release Year"),record.get("Genre"),record.get("Cast and Crew"),record.get("IMDB rating"),record.get("Duration"),record.get("Image URL"),record.get("Platform"))
        
        self.cursor.execute(query,val)
        self.db.commit()
    
    def close_spider(self,spider):
        self.cursor.close()
        self.db.close()

    def create_connection(self):
        try:
            self.db = mysql.connect(
                host = self.configuration['db_host'],
                user = self.configuration['db_username'],
                password = self.configuration['db_password'],
                database = self.configuration['db_name']
            )
            self.cursor = self.db.cursor()
            print("Database Connection Formed")
        except:
            print("Error in Database Connection")
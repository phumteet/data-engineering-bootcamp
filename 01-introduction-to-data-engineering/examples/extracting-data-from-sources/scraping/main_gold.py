import csv

import scrapy
from scrapy.crawler import CrawlerProcess


URL = "https://ทองคําราคา.com/"


class MySpider(scrapy.Spider):
    name = "gold_price_spider"
    start_urls = [URL,]

    def parse(self, response):
        td_list = []

        header = response.css("#divDaily h3::text").get().strip()
        print(header)

        table = response.css("#divDaily .pdtable")
        # print(table)

        rows = table.css("tr")
        # rows = table.xpath("//tr")
        # print(rows)

        for row in rows:
            td_texts = row.css("td::text").extract()
            # print(row.css("td::text").extract())
            # print(row.xpath("td//text()").extract())
            td_texts_array = list(td_texts)
            td_list.append(td_texts_array)
            
        print(td_list)
        # Write to CSV
        # YOUR CODE HERE
        # file_path_csv = './../03-data-lake-with-google-cloud-storage/examples/gold_price.csv'
        with open('gold_price.csv','w', newline='') as csvfile:
            # Create a CSV writer object
            csvwriter = csv.writer(csvfile)
    
            # Write the data to the CSV file
            for row in td_list:
                csvwriter.writerow(row)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(MySpider)
    process.start()

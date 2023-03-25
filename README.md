
# OTT PLATFORM CONTENT SCRAPER

Scraper to fetch data points from site & showing that through API endpoint.


## Scraper Execution

To run Scraper, run the following command

```python
  scrapy crawl <spider_name>
```
where **spider names** could be comparetv, or justwatch. 



## Start Server

To run Server, run the following command

```python
  uvicorn apis:app --reload
```
Open your browser at **http://127.0.0.1:8000** to see the JSON response **OR** go to **http://127.0.0.1:8000/docs** to open that in Swagger UI.

## API Reference

#### Get items

```http
  GET /
```

| Parameter | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `site` | `string` | **Optional**. Site name |
| `offset` | `int` | **Optional**. Start value |
| `limit` | `int` | **Optional**. End value |



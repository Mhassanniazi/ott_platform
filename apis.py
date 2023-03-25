from fastapi import FastAPI, HTTPException, Query
from OTT_platform.pipelines import OttPlatformPipeline


# fastapi instance 
app = FastAPI()

# database connection 
dbCon = OttPlatformPipeline()

# endpoints to get records data
@app.get("/")
def Show_records(site: str = Query('comparetv',description="available sites: comparetv & justwatch"), offset: int=0, limit: int=10):
    if limit >50:
        raise HTTPException(status_code=400, detail="requested data limit exceeded")
    else:
        dbCon.cursor.execute("""SELECT * FROM records WHERE site_name=%s LIMIT %s,%s""",(site,offset,limit))
        dataset = dbCon.cursor.fetchall()
        
        ott_data = list(map(lambda x: {'id':x[0], 'site_name':x[1], 'title':x[2], 'description':x[3], 'release_year':x[4], 'genre':x[5], 'cast':x[6], 'imdb_rating':x[7], 'duration':x[8], 'image':x[9], 'platform':x[10], 'insertion_time':x[11]}, dataset))
        return ott_data


import json
import pandas as pd
import requests
import time

# Reading manga IDs from the JSON file
with open("manga_cache.json", 'r') as f:
    manga_id = json.load(f)["sfw"]

def get_infos(id, df, response, count):
    print(f"Getting information of manga with id {id}  No of iterations : {count}")
    data = response.json()["data"]
    
    mal_id = data.get("mal_id")
    image = data["images"]["jpg"]["image_url"]
    title = data["titles"][0]["title"]
    # score = data.get("score", 0)
    score = data.get("score", 0) if data.get("score") is not None else 0
    rank = data.get("rank")
    synopsis = data.get("synopsis", "")
    background = data.get("background", "")
    genres = [i["name"] for i in data.get("genres", [])]
    themes = [i["name"] for i in data.get("themes", [])]
    demographics = [i["name"] for i in data.get("demographics", [])]
    status = data.get("status", "Unknown")
    authors = [i["name"] for i in data.get("authors", [])]
    chapters = data.get("chapters")
    volumes = data.get("volumes")
    
    row = [mal_id, image, title, score, rank, synopsis, background, genres, themes, demographics, status, authors, chapters, volumes]
    df.loc[len(df.index)] = row

def get_data(mal_ids):
    endpoint = "https://api.jikan.moe/v4/manga/{id}/full"
    df = pd.DataFrame(columns=["id", "image_url", "title", "score", "rank", "synopsis", "background", "genres", "themes", "demographics", "status", "authors", "chapters", "volumes"])
    count = 0
    for i in range(0, len(mal_ids), 3):
        batch_ids = mal_ids[i:i+3]  # Processing 3 IDs at a time
        for id in batch_ids:
            try:
                response = requests.get(endpoint.format(id=id))
                if response.status_code == 200:
                    get_infos(id, df, response, count)
                    count += 1
                else:
                    # Retry if the request fails
                    print(f"Failed request for ID {id}. Retrying...")
                    time.sleep(1)  # Wait 1 second before retrying
                    response = requests.get(endpoint.format(id=id))
                    if response.status_code == 200:
                        get_infos(id, df, response, count)
                        count += 1
                    else:
                        print(f"Failed again for ID {id}. Skipping.")
            except Exception as e:
                print(f"Error fetching data for ID {id}: {e}")
    return df

# Fetching dataset for the manga IDs
dataset = get_data(manga_id)
dataset.to_csv("manga_dataset.csv", index=False)

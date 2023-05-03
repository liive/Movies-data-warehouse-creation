import requests
import pandas as pd
import os


class Scrapping:
    def __init__(self, max_pages, api_key):
        self.max_pages = max_pages
        self.api_key = api_key

    def get_url(self, current_page):
        return f"https://api.themoviedb.org/3/discover/movie?api_key={self.api_key}&language=en-US&sort_by=popularity.desc&include_adult=false&include_video=false&page={current_page}&with_watch_monetization_types=flatrate&year=2022"

    def scraping_data(self):
        all_data = []
        first_request = True
        current_page = 1

        while current_page <= self.max_pages:

            url = self.get_url(current_page)
            response = requests.get(url)
            if first_request:
                print(response.status_code)
                first_request = False
            if response.status_code != 200:
                print(f"Request failed with status code {response.status_code}")
                break

            data = response.json()

            if current_page >= self.max_pages:
                break

            results = data["results"]
            all_data.extend(results)

            current_page += 1

        df = pd.DataFrame(all_data)
        if os.path.exists("data.csv"):
            os.remove("data.csv")

        df.to_csv("data.csv", index=False)


def main():
    max_pages = 5
    api_key = "711396eb995af5a58d92a5ac1242c30f"
    sc = Scrapping(max_pages, api_key)
    # sc.scraping_data()
    print("sc ", sc.max_pages)
    asdsa = Scrapping(10, api_key)
    print("asdsa ", asdsa.max_pages)


if __name__ == "__main__":
    main()


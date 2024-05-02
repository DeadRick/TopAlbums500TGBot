import requests
from bs4 import BeautifulSoup

from db import DbHandler

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}



def get_albums(url, article_num=500):
    try:
        albums = []

        print("IM IN GET_ALBUMS")
        response = requests.get(url[0], headers=headers, allow_redirects=False)
        response.raise_for_status()  # проверяем на ошибки

        # Создаем объект BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Поиск тегов <article>
        articles = soup.find_all('article')
        articles = articles[::-1]
        print(f"Найдено {len(articles)} элементов <article>:")
        # Итерация по каждому article
        for index, article in enumerate(articles[:-1]):
            # Поиск изображения
            image = article.find('img')
            if image is None:
                continue
            image = image['data-lazy-src']
            image = image.replace('w=300', 'w=600')

            # Поиск номера альбома
            album_number = article_num
            article_num -= 1

            # Поиск названия альбома
            title = article.find('h2').text.strip()

            # Поиск описания альбома
            description = article.find('p').text.strip()
            DbHandler().add_album(image, title, description)
            albums.append((image, album_number, title, description))
        return albums
    except requests.exceptions.HTTPError as e:
        print("HTTP Error:", e)
    except requests.exceptions.RequestException as e:
        print("Error Accessing:", e)

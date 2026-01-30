"""
Scrapy-скрипт для сбора фильмов с
https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту

Собираемые поля: title, genre, director, country, year
Запись: films.csv (UTF-8)
"""

from urllib.parse import quote_plus, unquote
import os
import re
import logging

import scrapy
from scrapy.crawler import CrawlerProcess

class FilmItem(dict):
    pass

class FilmsSpider(scrapy.Spider):
    name = "films_spider"
    allowed_domains = ["ru.wikipedia.org"]
    start_urls = [
        "https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"
    ]

    custom_settings = {
        "FEEDS": {
            "films.csv": {
                "format": "csv",
                "encoding": "utf-8",
                "fields": ["title", "genre", "director", "country", "year"],
            }
        },
        "FEED_EXPORT_ENCODING": "utf-8",
        "ROBOTSTXT_OBEY": True,
        "DOWNLOAD_DELAY": 0.8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "LOG_LEVEL": "INFO",
        "USER_AGENT": "ScrapyFilmsBot/1.0 (+https://example.com) Python/Scrapy",
    }

    def parse(self, response):
        """Парсим страницу категории: подкатегории, списки страниц (фильмов), пагинация."""
        self.logger.info("Category page: %s", response.url)

        # Подкатегории
        subcats = response.xpath('//div[@id="mw-subcategories"]//a/@href').getall()
        for href in subcats:
            url = response.urljoin(href)
            yield scrapy.Request(url, callback=self.parse)

        # Страницы в категории
        page_links = response.xpath('//div[@id="mw-pages"]//div[contains(@class,"mw-category-group")]//a/@href').getall()
        for href in page_links:
            # Пропускаем namespace-пути
            if ":" in href.split("/wiki/")[-1]:
                continue
            url = response.urljoin(href)
            yield scrapy.Request(url, callback=self.parse_article)

        # Пагинация
        next_page = response.xpath('//a[contains(normalize-space(.),"Следующая")]/@href').get()
        if not next_page:
            next_page = response.xpath('//link[@rel="next"]/@href').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_article(self, response):
        """Парсим статью — сначала проверяем, что это фильм, затем извлекаем поля."""
        self.logger.info("Article: %s", response.url)

        # Заголовок: берём все текстовые узлы внутри #firstHeading
        heading_parts = response.css('#firstHeading ::text').getall() or []
        title = ' '.join([p.strip() for p in heading_parts if p.strip()]).strip()

        # Если заголовок пуст — делаем корректный декод из URL
        if not title:
            raw = response.url.split('/wiki/')[-1]
            raw = raw.replace('_', ' ')
            try:
                title = unquote(raw)
            except Exception:
                # fallback — просто заменим % на пробелы
                title = raw

        if not title:
            # На всякий случай, если и это не сработало
            title = "не указано"

        # Проверяем — является ли страница фильмом
        if not self.is_film_page(response):
            self.logger.debug("Skipped (not a film): %s", title)
            return

        # Инициализируем поля с пометкой 'не указано'
        item = FilmItem({
            "title": title,
            "genre": "не указано",
            "director": "не указано",
            "country": "не указано",
            "year": "не указано",
        })

        # Разбор инфобокса
        infobox_rows = response.xpath("//table[contains(@class,'infobox')]//tr")
        for row in infobox_rows:
            th = ''.join(row.xpath('./th//text()').getall() or []).strip()
            td_parts = [t.strip() for t in row.xpath('./td//text()').getall() or [] if t.strip()]
            td = ', '.join(td_parts).strip() if td_parts else ''

            if not th:
                continue
            th_lower = th.lower()

            if 'жанр' in th_lower:
                if td:
                    item['genre'] = td
            elif 'режис' in th_lower:
                if td:
                    item['director'] = td
            elif 'стра' in th_lower:  # Страна
                if td:
                    item['country'] = td
            elif 'год' in th_lower:
                if td:
                    # ищем ближайший год
                    m = re.search(r'(\d{4})', td)
                    if m:
                        item['year'] = m.group(1)
                    else:
                        item['year'] = td

        # Попытка взять год из первого абзаца, если по-прежнему 'не указано'
        if item['year'] == 'не указано':
            first_pars = response.xpath('//div[@class="mw-parser-output"]/p[not(@class) and normalize-space()]//text()').getall()
            first_text = ' '.join([t.strip() for t in first_pars]).strip()
            m = re.search(r'(\d{4})', first_text)
            if m:
                item['year'] = m.group(1)
        yield item

    def is_film_page(self, response):
        """Определяем, является ли страница фильмом.
        Требуем наличие инфобокса + хотя бы одного признака 'фильм' (caption, начало текста, категории, th инфобокса).
        """
        # Обязательно: инфобокс
        has_infobox = bool(response.xpath("//table[contains(@class,'infobox')]"))
        if not has_infobox:
            return False

        # 1) подпись infobox
        caption = response.xpath("//table[contains(@class,'infobox')]//caption//text()").get()
        if caption and 'фильм' in caption.lower():
            return True

        # 2) первый абзац
        first_pars = response.xpath('//div[@class="mw-parser-output"]/p[not(@class) and normalize-space()]//text()').getall()
        first_text = ' '.join(first_pars[:200]).lower() if first_pars else ''
        if 'фильм' in first_text:
            return True

        # 3) заголовок первой строки инфобокса
        first_th = response.xpath("//table[contains(@class,'infobox')]//tr/th//text()").get()
        if first_th and 'фильм' in first_th.lower():
            return True

        # 4) категории
        cats = [c.strip().lower() for c in response.css('div#mw-normal-catlinks a::text').getall() or []]
        for c in cats:
            if 'фильм' in c or 'фильмы' in c:
                return True
        return False

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(FilmsSpider)
    process.start()

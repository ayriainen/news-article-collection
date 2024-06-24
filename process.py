from concurrent.futures import ThreadPoolExecutor, as_completed
from newspaper import Article

import database

def download_and_parse( url ):
    try:
        article = Article( url )
        article.download()
        article.parse()

        new_article = database.articles.insert().values(
            url = url,
            html = article.html,
            full_text = article.text,
            time = article.publish_date
        )

        new_article = database.connection.execute( new_article )
        return new_article.lastrowid
    except Exception as e:
        print(f"Failed to process {url}: {e}")
        return None

def process_url( row ):
    row = row._mapping

    stm = database.urls.update().where( database.urls.c.id == row['id'] ).values( download_attempted=True )
    database.connection.execute( stm )

    try:
        stored_id = download_and_parse( row['url'] )
        if stored_id:
            stm = database.urls.update().where( database.urls.c.id == row['id'] ).values( article_id=stored_id )
            database.connection.execute( stm )
    except Exception as e:
        print(f"Error processing URL {row['url']}: {e}")

def process_urls():
    urls_to_collect = database.urls.select().where( database.urls.c.download_attempted == False )
    urls_to_collect = database.connection.execute( urls_to_collect ).fetchall()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_url, row): row for row in urls_to_collect}

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error in future {futures[future]['url']}: {e}")

    database.connection.commit()

if __name__ == '__main__':
    process_urls()

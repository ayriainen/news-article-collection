from concurrent.futures import ThreadPoolExecutor, as_completed
from newspaper import Article
import time

import database

def download_and_parse( url, session ):
    try:
        article = Article( url )

        # retries for when the network coughs, not foolproof however
        # perhaps increase sleep time to improve catch rate
        retries = 3
        for attempt in range(retries):
            try:
                article.download()
                break
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    raise

        article.parse()

        new_article = database.articles.insert().values(
            url = url,
            html = article.html,
            full_text = article.text,
            time = article.publish_date
        )

        new_article = session.execute( new_article )
        return new_article.lastrowid
    except Exception as e:
        print(f"Failed to process {url}: {e}")
        return None

def process_url( row, session ):
    row = row._mapping

    # mark url as download attempted
    stm = database.urls.update().where( database.urls.c.id == row['id'] ).values( download_attempted = True )
    session.execute( stm )

    # attempt download
    try:
        stored_id = download_and_parse( row['url'], session )
        if stored_id:
            stm = database.urls.update().where( database.urls.c.id == row['id'] ).values( article_id = stored_id )
            session.execute( stm )
    except Exception as e:
        print(f"Error processing URL {row['url']}: {e}")

def process_urls():
    with database.session_scope() as session:
        urls_to_collect = session.query( database.urls ).filter( database.urls.c.download_attempted == False ).all()

        # thread pool with up to max_workers number threads concurrently
        # worker number can be adjusted to fit the system
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(process_url, row, session): row for row in urls_to_collect}

            for future in as_completed(futures):
                row = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"Error in future for URL {row['url']}: {e}")

        session.commit()

if __name__ == '__main__':
    process_urls()
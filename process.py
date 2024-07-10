import sys
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
        print(f"Error downloading and parsing article {url}: {e}", file=sys.stderr)
        return None

def process_urls():
    try:
        urls_to_collect = database.urls.select().where( database.urls.c.download_attempted == False )
        urls_to_collect = database.connection.execute( urls_to_collect ).fetchall()
    except Exception as e:
        print(f"Error fetching URLs from database: {e}", file=sys.stderr)
        return

    download_and_parse("v")

    for row in urls_to_collect:

        row = row._mapping 

        try:
            stm = database.urls.update().where( database.urls.c.id == row['id'] ).values( download_attempted = True )
            database.connection.execute( stm )
        except Exception as e:
            print(f"Error updating download_attempted for URL ID {row['id']}: {e}", file=sys.stderr)
            continue

        try:
            stored_id = download_and_parse( row['url'] )
            stm = database.urls.update().where( database.urls.c.id == row['id'] ).values( article_id = stored_id )
            database.connection.execute( stm )
        except Exception as e:
            print(f"Error processing URL {row['url']}: {e}", file=sys.stderr)

        try:
            database.connection.commit()
        except Exception as e:
            print(f"Error committing processing: {e}", file=sys.stderr)

if __name__ == '__main__':
    process_urls()

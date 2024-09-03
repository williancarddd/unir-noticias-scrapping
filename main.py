import time
import asyncio
import urllib3
from os.path import exists
from scrapping import scrap, NewsDatabase
from discord_bot.DiscordBot import DiscordBot  # Supondo que a classe DiscordBot esteja em discord_bot.py
import pandas


def init(props):
    db = NewsDatabase(db_name="news.db")

    for prop in props:
        tablename = prop.pop("tablename")

        db.load_dataframe(
            scrap(
                **prop,
            ),
            tablename,
        )

    db.disconnect()


def monitoring(props):
    db = NewsDatabase(db_name="news.db")

    for prop in props:
        tablename = prop.pop("tablename")
        
        # Tentar realizar a inserção, mas se a tabela não existir, chamar init para criar as tabelas
        try:
            db.insert_difference(
                scrap(
                    **prop,
                ),
                tablename,
            )
        except pandas.errors.DatabaseError as e:
            # Se a tabela não existir, chamar init para criar as tabelas
            print(f"Tabela {tablename} não encontrada. Criando a tabela...")
            db.load_dataframe(
                scrap(
                    **prop,
                ),
                tablename,
            )

    db.disconnect()


# Desativar os avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configurações do Discord
DISCORD_TOKEN = ""
DISCORD_GUILD = 844023786482171916  # Certifique-se de que este é um número e não uma string
DISCORD_CHANNEL = "noticias-unir"  # Certifique-se de que o nome do canal é válido no Discord
bot = DiscordBot(DISCORD_TOKEN, DISCORD_GUILD)

async def run_scraping_and_send_news():
    scrap_props = [
        {
            "url": "https://dacc.unir.br/noticia/pagina",
            "classes": "col-md-6 col-lg-6 py-2",
            "tablename": "dacc_news",
        },
        {
            "url": "http://www.unir.br/noticia/lista_comunicados",
            "classes": "col-6 mt-4 pl-3",
            "tablename": "announcements",
        },
        {
            "url": "http://www.unir.br/noticia/lista_noticias",
            "classes": "col-md-6 pl-3",
            "tablename": "unir_news",
        },
    ]

    if not exists("news.db"):
        init(scrap_props)
    else:
        monitoring(scrap_props)

    # Coletar as notícias mais recentes
    all_news = []
    for prop in scrap_props:
        news_df = scrap(**prop)
        all_news.extend(news_df.values.tolist())

    # Criar o canal (se necessário) e enviar as notícias
    await bot.create_channel(DISCORD_CHANNEL)
    await bot.send_news(DISCORD_CHANNEL, all_news)

async def main_loop():
    while True:
        # Executar o scraping e enviar as notícias para o Discord
        await run_scraping_and_send_news()
        
        # Espera por 5 minutos (300 segundos)
        await asyncio.sleep(300)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        # Inicializar e rodar o bot junto com a função principal
        loop.create_task(bot.client.start(DISCORD_TOKEN))
        loop.run_until_complete(main_loop())
    finally:
        loop.run_until_complete(bot.client.logout())
        loop.close()

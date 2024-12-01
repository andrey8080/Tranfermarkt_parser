import requests
from bs4 import BeautifulSoup
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging
from urllib.parse import urljoin
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://www.transfermarkt.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Connection": "keep-alive"
}
MAX_WORKERS = 10


def get_clubs(url):
    """Получает список команд из таблицы на главной странице."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=20)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Ошибка при запросе {url}: {e}")
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', class_='items')
    rows = table.find('tbody').find_all('tr') if table else []
    clubs = []

    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 7:
            club_cell = cells[1].find('a')
            if club_cell:
                club_name = club_cell.get_text(strip=True)
                club_link = f"{BASE_URL}{club_cell['href']}"
                clubs.append({"name": club_name, "link": club_link})

    logging.info(f"Найдено клубов: {len(clubs)}")
    return clubs

def get_player_stats(profile_link):
    """Получает статистику игрока."""
    try:
        time.sleep(2)  # Добавляем задержку между запросами
        response = requests.get(profile_link, headers=HEADERS, timeout=80)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Ошибка при запросе {profile_link}: {e}")
        return {"position": "Unknown", "total_stats": {}}
    
    soup = BeautifulSoup(response.content, 'html.parser')

    position = "Unknown"
    data_header_items = soup.find_all('li', class_='data-header__label')
    if data_header_items:
        for item in data_header_items:
            if 'Position:' in item.text:
                position_tag = item.find('span', class_='data-header__content')
                if position_tag:
                    position = position_tag.text.strip()
                    break

    detailed_stats = {}
    stats_table = soup.find('table', class_='items')
    if stats_table:
        rows = stats_table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if not cells:
                continue
            first_cell_text = cells[0].get_text(strip=True)
            if first_cell_text.startswith('Total'):
                if position.lower() == "goalkeeper":
                    detailed_stats = {
                        "Appearances": cells[2].get_text(strip=True),
                        "Goals": cells[3].get_text(strip=True),
                        "Own goals": cells[4].get_text(strip=True),
                        "Substitutions on": cells[5].get_text(strip=True),
                        "Substitutions off": cells[6].get_text(strip=True),
                        "Yellow cards": cells[7].get_text(strip=True),
                        "Second yellow cards": cells[8].get_text(strip=True) if len(cells) > 8 else "-",
                        "Red cards": cells[9].get_text(strip=True) if len(cells) > 9 else "-",
                        "Goals conceded": cells[10].get_text(strip=True) if len(cells) > 10 else "-",
                        "Clean sheets": cells[11].get_text(strip=True) if len(cells) > 11 else "-",
                        "Minutes played": cells[12].get_text(strip=True) if len(cells) > 12 else "-"
                    }
                else:
                    detailed_stats = {
                        "Appearances": cells[2].get_text(strip=True),
                        "Goals": cells[3].get_text(strip=True),
                        "Assists": cells[4].get_text(strip=True),
                        "Own goals": cells[5].get_text(strip=True),
                        "Substitutions on": cells[6].get_text(strip=True),
                        "Substitutions off": cells[7].get_text(strip=True),
                        "Yellow cards": cells[8].get_text(strip=True),
                        "Second yellow cards": cells[9].get_text(strip=True) if len(cells) > 9 else "-",
                        "Red cards": cells[10].get_text(strip=True) if len(cells) > 10 else "-",
                        "Penalty goals": cells[11].get_text(strip=True) if len(cells) > 11 else "-",
                        "Minutes per goal": cells[12].get_text(strip=True) if len(cells) > 12 else "-",
                        "Minutes played": cells[13].get_text(strip=True) if len(cells) > 13 else "-"
                    }
                break

    return {
        "position": position,
        "total_stats": detailed_stats
    }

def process_player(row):
    """Обрабатывает одного игрока и возвращает его данные."""
    name_cell = row.find('td', class_='hauptlink')
    cost_cell = row.find('td', class_='rechts hauptlink')
    if name_cell and cost_cell:
        name_tag = name_cell.find('a')
        if name_tag:
            name = name_tag.text.strip()
            href = name_tag.get('href', '')
            if not href:
                logging.warning(f"Ссылка отсутствует для игрока: {name}")
                return None
            profile_href = href.replace('/profil/', '/leistungsdaten/') + '/plus/1?saison=ges'
            profile_link = urljoin(BASE_URL, profile_href)
            cost = cost_cell.text.strip()

            logging.info(f"Обработка игрока: {name}, ссылка: {profile_link}")

            player_data = get_player_stats(profile_link)

            return {
                "name": name,
                "position": player_data["position"],
                "cost": cost,
                "profile_link": profile_link,
                "detailed_stats": player_data["total_stats"]
            }
    return None

def get_players(club_link):
    """Получает список игроков клуба."""
    try:
        response = requests.get(club_link, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Ошибка при запросе {club_link}: {e}")
        return []
    
    soup = BeautifulSoup(response.content, 'html.parser')
    players = []

    player_rows = soup.find_all('tr', class_='odd') + soup.find_all('tr', class_='even')
    if not player_rows:
        logging.warning(f"Игроки не найдены на странице клуба: {club_link}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_player = {executor.submit(process_player, row): row for row in player_rows}
        for future in as_completed(future_to_player):
            try:
                player_data = future.result()
                if player_data:
                    players.append(player_data)
            except Exception as e:
                row = future_to_player[future]
                logging.error(f"Ошибка при обработке игрока: {e}")

    logging.info(f"Найдено игроков: {len(players)} для клуба: {club_link}")
    return players

def process_club(club):
    """Обрабатывает команду и возвращает ее данные."""
    logging.info(f"Парсинг команды: {club['name']}")
    players = get_players(club['link'])
    logging.info(f"Обработано игроков: {len(players)} для команды: {club['name']}")
    return {
        "club_name": club["name"],
        "club_link": club["link"],
        "players": players
    }

def main():
    parser = argparse.ArgumentParser(description="Парсер клубов лиги")
    parser.add_argument('league_url', type=str, help='URL лиги')
    args = parser.parse_args()

    league_url = args.league_url
    clubs = get_clubs(league_url)

    if not clubs:
        logging.error("Клубы не найдены.")
        return

    league_data = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_club = {executor.submit(process_club, club): club for club in clubs}
        for future in as_completed(future_to_club):
            try:
                club_data = future.result()
                league_data.append(club_data)
            except Exception as e:
                club = future_to_club[future]
                logging.error(f"Ошибка при обработке клуба {club['name']}: {e}")

    with open('output.json', 'w', encoding='utf-8') as f:
        json.dump(league_data, f, ensure_ascii=False, indent=4)

    logging.info("Парсинг завершен успешно!")

if __name__ == "__main__":
    main()

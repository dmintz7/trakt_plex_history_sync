import plex, logging, os, sys, config, sqlite3, pymysql, warnings
from trakt import TraktImporter
from logging.handlers import RotatingFileHandler
from datetime import datetime

def create_episode_insert(episodes):
	logger.info("Inserting Episodes")
	for x in episodes:
		query = "INSERT IGNORE INTO trakt_episodes (id,showTitle,showYear,seasonNumber,episodeNumber,episodeTitle,showTrakt,showSlug,showTVDB,showIMDB,showTMDB,showTVRage,episodeTrakt,episodeTVDB,episodeIMDB,episodeTMDB,episodeTVRage,watched_at) VALUES (%s,'%s',%s,%s,%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" % (x['id'], pymysql.escape_string(x['show']['title']), x['show']['year'], x['episode']['season'], x['episode']['number'], pymysql.escape_string(x['episode']['title']), x['show']['ids']['trakt'], x['show']['ids']['slug'], x['show']['ids']['tvdb'], x['show']['ids']['imdb'], x['show']['ids']['tmdb'], x['show']['ids']['tvrage'], x['episode']['ids']['trakt'], x['episode']['ids']['tvdb'], x['episode']['ids']['imdb'], x['episode']['ids']['tmdb'], x['episode']['ids']['tvrage'], datetime.fromisoformat(x['watched_at'][:-1]))
		mysql_insert(query)

def create_movie_insert(movies):
	logger.info("Inserting Movies")
	for x in movies:
		query = "INSERT IGNORE INTO trakt_movies (id, title,`year`,tmdb,imdb,slug,trakt,watched_at) VALUES (%s,'%s',%s,'%s','%s','%s','%s','%s');" % (x['id'], pymysql.escape_string(x['movie']['title']), x['movie']['year'], x['movie']['ids']['tmdb'], x['movie']['ids']['imdb'], x['movie']['ids']['slug'], x['movie']['ids']['trakt'], datetime.fromisoformat(x['watched_at'][:-1]))
		mysql_insert(query)

def mysql_insert(query):
	with warnings.catch_warnings():
		warnings.simplefilter("ignore")
		conn = pymysql.connect(host=config.host, port=config.port, user=config.user, password=config.passwd, database=config.dbname)
		cursor = conn.cursor()
		query = query.replace("'None'", "Null")
		cursor.execute(query)
		conn.commit()
		id = cursor.lastrowid
		cursor.close()
		conn.close()
	return id
	
def mysql_select(query):
	conn = pymysql.connect(host=config.host, port=config.port, user=config.user, password=config.passwd, database=config.dbname)
	cursor = conn.cursor()
	cursor.execute(query)
	results = cursor.fetchall()
	cursor.close()
	conn.close()
	return results

def plex_insert(query):
	conn = sqlite3.connect(config.path_to_plex_db)
	cursor = conn.cursor()
	query = query.replace("'None'", "Null")
	cursor.execute(query)
	conn.commit()
	id = cursor.lastrowid
	cursor.close()
	conn.close()
	return id
	
def plex_select(query, count=0):
	conn = sqlite3.connect(config.path_to_plex_db)
	cursor = conn.cursor()
	cursor.execute(query)
	if count:
		results = cursor.fetchone()
	else:
		results = cursor.fetchall()
	cursor.close()
	conn.close()
	return results

def import_trakt(page_max=0):
	importer = TraktImporter()
	if importer.authenticate():
		movies = importer.get_movie_list('history', page_max)
		logger.info("%s Movies Found" % len(movies))
		create_movie_insert(movies)
		episodes = importer.get_episode_list('history', page_max)
		logger.info("%s Episodes Found" % len(episodes))
		create_episode_insert(episodes)
		
def initialize_database():
	conn = pymysql.connect(host=config.host, port=config.port, user=config.user, password=config.passwd)
	cursor = conn.cursor()
	cursor.execute("SHOW DATABASES")
	if not config.dbname in str(cursor.fetchall()):
		logger.info("Create Database")
		mysql_insert("CREATE DATABASE %s" % config.dbname)

		logger.info("Create Tables")
		mysql_insert("CREATE TABLE `compare` ( `id` int NOT NULL AUTO_INCREMENT, `trakt_id` bigint unsigned DEFAULT NULL, `plex_id` bigint unsigned DEFAULT NULL, `date_added` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`), UNIQUE KEY `trakt_id_UN` (`trakt_id`), UNIQUE KEY `plex_id_UN` (`plex_id`) ) ENGINE=InnoDB AUTO_INCREMENT=763820 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;")
		mysql_insert("CREATE TABLE `plex_views` ( `id` int NOT NULL, `account_id` int DEFAULT NULL, `guid` varchar(255) DEFAULT NULL, `metadata_type` int DEFAULT NULL, `library_section_id` int DEFAULT NULL, `grandparent_title` varchar(255) DEFAULT NULL, `parent_index` int DEFAULT NULL, `parent_title` varchar(255) DEFAULT NULL, `index` int DEFAULT NULL, `title` varchar(255) DEFAULT NULL, `thumb_url` varchar(255) DEFAULT NULL, `viewed_at` datetime DEFAULT NULL, `grandparent_guid` varchar(255) DEFAULT NULL, `originally_available_at` datetime DEFAULT NULL, `device_id` int DEFAULT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;")
		mysql_insert("CREATE TABLE `trakt_episodes` ( `id` bigint unsigned NOT NULL, `showTitle` text, `showYear` int DEFAULT NULL, `seasonNumber` int DEFAULT NULL, `episodeNumber` int DEFAULT NULL, `episodeTitle` text, `showTrakt` varchar(100) DEFAULT NULL, `showSlug` text, `showTVDB` varchar(100) DEFAULT NULL, `showIMDB` varchar(100) DEFAULT NULL, `showTMDB` varchar(100) DEFAULT NULL, `showTVRage` varchar(100) DEFAULT NULL, `episodeTrakt` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL, `episodeTVDB` varchar(100) DEFAULT NULL, `episodeIMDB` varchar(100) DEFAULT NULL, `episodeTMDB` varchar(100) DEFAULT NULL, `episodeTVRage` varchar(100) DEFAULT NULL, `watched_at` datetime DEFAULT NULL, `date_added` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;")
		mysql_insert("CREATE TABLE `trakt_movies` ( `id` bigint unsigned NOT NULL, `title` text, `year` int DEFAULT NULL, `tmdb` varchar(100) DEFAULT NULL, `imdb` varchar(100) DEFAULT NULL, `slug` text, `trakt` varchar(100) DEFAULT NULL, `watched_at` datetime DEFAULT NULL, `date_added` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;")

		logger.info("Create View")
		mysql_insert("CREATE OR REPLACE ALGORITHM = UNDEFINED VIEW `plex_trakt_match` AS select pv.guid AS guid, ifnull(ifnull(movies_imdb.id, movies_tmdb.id), ifnull(episodes_tmdb.id, episodes_tvdb.id)) AS trakt_id, pv.id AS plex_id, ifnull(ifnull(movies_imdb.imdb, movies_tmdb.tmdb), ifnull(episodes_tvdb.showTVDB, episodes_tmdb.showTMDB)) AS web_id, pv.grandparent_title AS grandparent_title, pv.title AS title, ifnull(episodes_tvdb.seasonNumber, episodes_tmdb.seasonNumber) AS seasonNumber, ifnull(episodes_tvdb.episodeNumber, episodes_tmdb.episodeNumber) AS episodeNumber, pv.viewed_at AS plex_view, convert_tz(ifnull(ifnull(movies_imdb.watched_at, movies_tmdb.watched_at), ifnull(episodes_tvdb.watched_at, episodes_tmdb.watched_at)), 'UTC', 'America/New_York') AS trakt_view, timestampdiff(SECOND, pv.viewed_at, convert_tz(ifnull(ifnull(movies_imdb.watched_at, movies_tmdb.watched_at), ifnull(episodes_tvdb.watched_at, episodes_tmdb.watched_at)), 'UTC', 'America/New_York')) AS time_diff from plex_views pv left join trakt_movies movies_imdb on (substr(substring_index(pv.guid, '//', 1), 20) = 'imdb:' and movies_imdb.imdb = substr(substring_index(pv.guid, '?', 1),(22 + length(substr(substring_index(pv.guid, '//', 1), 20))))) left join trakt_movies movies_tmdb on (substr(substring_index(pv.guid, '//', 1), 20) = 'themoviedb:' and movies_tmdb.tmdb = substr(substring_index(pv.guid, '?', 1),(22 + length(substr(substring_index(pv.guid, '//', 1), 20))))) left join trakt_episodes episodes_tvdb on (substr(substring_index(pv.guid, '//', 1), 20) = 'thetvdb:'and concat(episodes_tvdb.showTVDB, '/', episodes_tvdb.seasonNumber, '/', episodes_tvdb.episodeNumber) = substr(substring_index(pv.guid, '?', 1),(22 + length(substr(substring_index(pv.guid, '//', 1), 20))))) left join trakt_episodes episodes_tmdb on (substr(substring_index(pv.guid, '//', 1), 20) = 'themoviedb:' and concat(episodes_tmdb.showTMDB, '/', episodes_tmdb.seasonNumber, '/', episodes_tmdb.episodeNumber) = substr(substring_index(pv.guid, '?', 1),(22 + length(substr(substring_index(pv.guid, '//', 1), 20)))));")
	cursor.close()
	conn.close()
	
if __name__ == '__main__':
	filename, file_extension = os.path.splitext(os.path.basename(__file__))
	formatter = logging.Formatter('%(asctime)s - %(levelname)10s - %(module)15s:%(funcName)30s:%(lineno)5s - %(message)s')
	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	consoleHandler = logging.StreamHandler(sys.stdout)
	consoleHandler.setFormatter(formatter)
	logger.addHandler(consoleHandler)
	logging.getLogger("requests").setLevel(logging.WARNING)
	logger.setLevel(config.log_level.upper())
	fileHandler = RotatingFileHandler(config.log_folder + '/trakt_plex.log', maxBytes=1024 * 1024 * 1, backupCount=1)
	fileHandler.setFormatter(formatter)
	logger.addHandler(fileHandler)
	
	logger.info("Starting Trakt-Plex Compare")
	
	initialize_database()
	import_trakt(1)

	plex.migrate_plex_table()
	plex.find_matches()
	
	plex.addWatched()
	
	plex.migrate_plex_table()
	plex.find_matches()

	plex.update_plex_times()
	logger.info("Finished Trakt-Plex Compare")
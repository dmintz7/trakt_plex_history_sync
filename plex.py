import sqlite3, main, config, logging, pymysql, os
from plexapi.server import PlexServer

logger = logging.getLogger('root')
plex = PlexServer(config.plex_host, config.plex_api)

def migrate_plex_table():
	logger.info("Migrating Plex Views Table")
	plex_ids = str([x for x, y in main.mysql_select("select id, viewed_at from plex_views")]).replace('[', '(').replace(']', ')')
	results = main.plex_select("SELECT id, account_id, guid, metadata_type, library_section_id, grandparent_title, parent_index, parent_title, `index`, title, thumb_url, viewed_at, grandparent_guid, originally_available_at, device_id FROM metadata_item_views where account_id = 1 and library_section_id in (1,2) and metadata_type in (1,4) and id not in %s;" % plex_ids)
	logger.info("Records to Migrate - %s" % len(results))
	for id, account_id, guid, metadata_type, library_section_id, grandparent_title, parent_index, parent_title, index, title, thumb_url, viewed_at, grandparent_guid, originally_available_at, device_id in results:
		if not grandparent_title: grandparent_title = ""
		if not parent_title: parent_title = ""
		if not title: title = ""
		main.mysql_insert("INSERT IGNORE INTO plex_views (id, account_id,guid,metadata_type,library_section_id,grandparent_title,parent_index,parent_title,`index`,title,thumb_url,viewed_at,grandparent_guid,originally_available_at,device_id) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" % (id, account_id,guid,metadata_type,library_section_id,pymysql.escape_string(grandparent_title),parent_index,pymysql.escape_string(parent_title),index,pymysql.escape_string(title),thumb_url,viewed_at,grandparent_guid,originally_available_at,device_id))
	logger.info("Finished Migrating Plex Views Table")

def find_matches():
	existing_trakt = []
	existing_plex = []
	logger.info("Add Exact Matches to Compare Table")
	main.mysql_insert("INSERT IGNORE INTO compare (plex_id, trakt_id) SELECT plex_id, trakt_id from plex_trakt_match WHERE time_diff = 0")
	results = main.mysql_select("SELECT * FROM plex_trakt_match WHERE (trakt_id not in (select trakt_id from compare) and plex_id not in (select plex_id from compare)) and time_diff is not null order by web_id, seasonNumber, episodeNumber, abs(time_diff)")
	logger.info("Add Matches to Compare Table - %s" % len(results))
	updated_count = 0
	for guid, trakt_id, plex_id, web_id, grandparent_title, title, seasonNumber, episodeNumber, plex_view, trakt_view, time_diff in results:
		if trakt_id not in existing_trakt and plex_id not in existing_plex:
			main.mysql_insert("INSERT IGNORE INTO compare (plex_id, trakt_id) VALUES ('%s','%s')" % (plex_id, trakt_id))
			existing_trakt.append(trakt_id)
			existing_plex.append(plex_id)
			updated_count+=1
	logger.info("%s Records Matches Added" % updated_count)
			
def addWatched():
	all = []
	for section in plex.library.sections():
		if section.TYPE in ('movie'):
			all = all + section.search()
		elif section.TYPE in ('show'):
			all = all + section.searchEpisodes()
	
	results = main.mysql_select("SELECT * FROM (SELECT id, concat('imdb://',imdb, '?'), concat('themoviedb://',tmdb, '?'), watched_at from trakt_movies UNION SELECT id, concat('thetvdb://',showTVDB, '/', seasonNumber, '/', episodeNumber, '?'), concat('themoviedb://',showTMDB, '/', seasonNumber, '/', episodeNumber, '?'), watched_at from trakt_episodes) tmp where id not in (select trakt_id from compare) and id not in (SELECT trakt_id FROM plex_trakt_match where trakt_id is not null)")
	if len(results) > 0:
		updated_count = 0
		logger.info("Adding New Records in Plex Database - %s Records" % len(results))
		for item in all:
			for trakt_id, imdtvb, tmdb, watched_at in results:
				if str(imdtvb) in item.guid or str(tmdb) in item.guid:
					item.markWatched()
					updated_count+=1
		logger.info("%s Records Added" % updated_count)
	else:
		logger.info("No Records to Add to Plex Database")
		
def update_plex_times():
	results = main.mysql_select("SELECT compare.plex_id, plex_trakt_match.trakt_view from compare join plex_trakt_match on (compare.trakt_id = plex_trakt_match.trakt_id and compare.plex_id = plex_trakt_match.plex_id) where time_diff != 0")
	if not len(plex.sessions()):
		logger.info("Stopping Plex Service")
		os.system("service plexmediaserver stop")

		logger.info("Updating Watched Time in Plex Database - %s Records" % len(results))
		conn = sqlite3.connect(config.path_to_db)
		cursor = conn.cursor()
		for plex_id, watched_at in results:
			insert_query = "UPDATE metadata_item_views SET viewed_at = '%s' WHERE id = '%s'" % (watched_at.strftime("%Y-%m-%d %H:%M:%S"), plex_id)
			main.mysql_insert("UPDATE plex_views SET viewed_at = '%s' WHERE id= '%s';" % (watched_at.strftime("%Y-%m-%d %H:%M:%S"), plex_id))
			cursor.execute(insert_query)
			conn.commit()
		cursor.close()
		conn.close()
		
		logger.info("Removing New Records in Plex Database with No Matches")
		plex_ids = str([x for x, y in main.mysql_select("select plex_id, trakt_id from compare")]).replace('[', '(').replace(']', ')')
		main.plex_insert("DELETE FROM metadata_item_views where account_id = 1 and library_section_id in (1,2) and id not in %s;" % plex_ids)
		main.plex_insert("DELETE FROM metadata_item_settings WHERE account_id = 1 and (account_id, guid) not in (SELECT account_id, guid FROM metadata_item_views)")
		main.mysql_insert("DELETE FROM plex_views WHERE id not in (select plex_id from compare)")
		

		logger.info("Updating records in metadata_item_settings")
		main.plex_insert("update metadata_item_settings set last_viewed_at = (select miv.viewed_at from (select guid, account_id, max(viewed_at) viewed_at from metadata_item_views group by guid, account_id) miv where miv.guid = metadata_item_settings.guid and miv.account_id = metadata_item_settings.account_id);")
		main.plex_insert("update metadata_item_settings set view_count = (select miv.count from (select guid, account_id, count() count from metadata_item_views group by guid, account_id) miv where miv.guid = metadata_item_settings.guid and miv.account_id = metadata_item_settings.account_id) where (account_id, guid) IN (select miv.account_id, miv.guid from (select guid, account_id, count() count from metadata_item_views group by guid, account_id) miv, metadata_item_settings mis where miv.guid = mis.guid and miv.account_id = mis.account_id and miv.count != mis.view_count);")

		logger.info("Starting Plex Service")
		os.system("service plexmediaserver start")
	else:
		logger.info("Plex Currently in Use. Not Updating")
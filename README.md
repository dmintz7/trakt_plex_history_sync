# trakt_plex_history_sync
 
Sync Plex Watch History with Trakt History

Requirements
* Python3
* MySQL Database

Acknowledgements
Portions of this script used logic originally published by anoopsankar at https://github.com/anoopsankar/Trakt2Letterboxd


What This Does
* Script will mark Plex episodes/movies as watched if watched in Trakt.
* If Trakt has no watch history, the history and watch status will be reset in Plex
* All watch history times will be updated in the Plex database to match the what is Trakt

This was created to solve a problem related to the Plex/Trakt sync. When a new episode/movie is added to Plex and the Trakt plugin is being used the watch history will be shown as when it was added.
With this script the date/time will show the actual watch date time of the episode/movie. This allows smart playlist using last watched filters to work more accurately.

To avoid corruption with the Plex database all deletes, inserts and updates will only occur when there are no active streams. When there are no streams, the Plex service will be stopped, the database will be updated. The Plex service will then be restarted


Future Updates
* Change to SQLite
	Currently much slower
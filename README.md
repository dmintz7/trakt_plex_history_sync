# trakt_plex_history_sync

Requirements
* Python3
* MySQL Database


Acknowledgements
Portions of this scripts use logic originally from [anoopsankar/Trakt2Letterboxd](https://github.com/anoopsankar/Trakt2Letterboxd)

This script will automatically update the datetime history for an episode/movie based on the datetime in trakt.

If the Plex item is unwatched, the item will be marked as watched and the datetime match Trakt. 

To avoid corrupting the Plex database, all updates, inserts and deletes to the Plex database will only occur if there are no active streams. The plex service will be stopped and then restarted once the script is complete. 

It is highly recommeneded to manually backup the plex database prior to running this script.

Future
* Change to SQLite
	Convert SQLite version is extremely slow
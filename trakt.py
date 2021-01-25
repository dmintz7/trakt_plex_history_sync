from urllib.request import Request, urlopen
from urllib.error import HTTPError
import json, time, os.path, config, logging

logger = logging.getLogger('root')

class TraktImporter(object):
	""" Trakt Importer """

	def __init__(self):
		self.api_root = 'https://api.trakt.tv'
		self.api_clid = config.client_id
		self.api_clsc = config.client_secret
		self.api_token = None

	def authenticate(self):
		""" Authenticates the user and grabs an API access token if none is available. """

		if self.__decache_token():
			return True
		dev_code_details = self.__generate_device_code()
		self.__show_auth_instructions(dev_code_details)
		got_token = self.__poll_for_auth(dev_code_details['device_code'], dev_code_details['interval'], dev_code_details['expires_in'] + time.time())

		if got_token:
			self.__encache_token()
			return True
		return False

	def __decache_token(self):
		if not os.path.isfile(os.path.join(os.path.dirname(os.path.abspath(__file__)), "t_token")): return False
		token_file = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "t_token"), 'r')
		self.api_token = token_file.read()
		token_file.close()
		return True

	def __encache_token(self):
		token_file = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "t_token"), 'w')
		token_file.write(self.api_token)
		token_file.close()

	@staticmethod
	def __delete_token_cache():
		os.remove(os.path.join(os.path.dirname(os.path.abspath(__file__)), "t_token"))

	def __generate_device_code(self):
		""" Generates a device code for authentication within Trakt. """
		request_body = """{{"client_id": "{0}"}}""".format(self.api_clid)
		request_headers = {'Content-Type': 'application/json'}
		request = Request(self.api_root + '/oauth/device/code', data=request_body.encode('utf-8'), headers=request_headers)
		response_body = urlopen(request).read()
		return json.loads(response_body)

	@staticmethod
	def __show_auth_instructions(details):
		message = ("\nGo to {0} on your web browser and enter the below user code there:\n\n"
				   "{1}\n\nAfter you have authenticated and given permission;"
				   "come back here to continue.\n"
				  ).format(details['verification_url'], details['user_code'])
		logger.info(message)

	def __poll_for_auth(self, device_code, interval, expiry):
		""" Polls for authorization token """
		request_headers = {'Content-Type': 'application/json'}
		request_body = """{{ "code":		  "{0}", "client_id":	 "{1}", "client_secret": "{2}" }}""".format(device_code, self.api_clid, self.api_clsc)
		request = Request(self.api_root + '/oauth/device/token', data=request_body.encode('utf-8'), headers=request_headers)
		response_body = ""
		should_stop = False
		logger.info("Waiting for authorization.")
		while not should_stop:
			time.sleep(interval)
			try:
				response_body = urlopen(request).read()
				should_stop = True
			except HTTPError as err:
				if err.code == 400:
					logger.info(".")
				else:
					logger.info("\n{0} : Authorization failed, please try again. Script will now quit.".format(err.code))
					should_stop = True
			should_stop = should_stop or (time.time() > expiry)

		if response_body:
			response_dict = json.loads(response_body)
			if response_dict and 'access_token' in response_dict:
				logger.info("Authenticated!")
				self.api_token = response_dict['access_token']
				logger.info("Token:" + self.api_token)
				return True
		return False

	def get_movie_list(self, list_name, page_max):
		""" Get movie list of the user. """
		logger.info("Getting Movie " + list_name)
		headers = {
			'Content-Type': 'application/json',
			'Authorization': 'Bearer ' + self.api_token,
			'trakt-api-version': '2',
			'trakt-api-key': self.api_clid
		}
		extracted_movies = []
		page_limit = 1
		page = 1

		while page <= page_limit:
			for attempt in range(5):
				request = Request(self.api_root + '/sync/' + list_name + '/movies?page={0}&limit=100'.format(page), headers=headers)
				try:
					response = urlopen(request)
					response_body = response.read()
					if response_body:
						extracted_movies+=json.loads(response_body)
				except HTTPError as err:
					if err.code == 401 or err.code == 403:
						logger.error("Auth Token has expired. Run Again to Reauthenticate.")
						self.__delete_token_cache()
						exit()
					logger.error("{0} An error occured.".format(err.code))
					time.sleep(5)
				else:
					if not page_max: 
						page_limit = int(response.getheader('X-Pagination-Page-Count'))
					else:
						page_limit = page_max
					logger.debug("Completed page {0} of {1}".format(page, page_limit))
					page+=1
					break
			else:
				logger.error("Failed 5 times")
		logger.info("All Movies Grabbed")
		return extracted_movies

	def get_episode_list(self, list_name, page_max):
		""" Get episode list of the user. """
		logger.info("Getting Episode " + list_name)
		headers = {
			'Content-Type': 'application/json',
			'Authorization': 'Bearer ' + self.api_token,
			'trakt-api-version': '2',
			'trakt-api-key': self.api_clid
		}

		extracted_episodes = []
		page_limit = 1
		page = 1

		while page <= page_limit:
			for attempt in range(5):
				request = Request(self.api_root + '/sync/' + list_name + '/episodes?page={0}&limit=100'.format(page), headers=headers)
				try:
					response = urlopen(request)
					response_body = response.read()
					if response_body:
						extracted_episodes+=json.loads(response_body)
				except HTTPError as err:
					if err.code == 401 or err.code == 403:
						logger.error("Auth Token has expired. Run Again to Reauthenticate.")
						self.__delete_token_cache()
						exit()
					logger.error("{0} An error occured.".format(err.code))
					time.sleep(5)
				else:
					if not page_max: 
						page_limit = int(response.getheader('X-Pagination-Page-Count'))
					else:
						page_limit = page_max
					logger.debug("Completed page {0} of {1}".format(page, page_limit))
					page+=1
					break
			else:
				logger.error("Failed 5 times")
				
		logger.info("All Episodes Grabbed")
		return extracted_episodes


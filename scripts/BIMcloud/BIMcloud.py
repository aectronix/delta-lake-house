import json
import requests

class BIMcloud():

	def __init__(self, user, password, config):

		self.conf = config['server']
		self.url = self.conf['url'] + ':' + str(self.conf['port'])
		self.user = user
		self.password = password
		self.auth = None

		self.connect()

	def connect(self):
		if not self.user or not self.password:
			raise ConnectionError(f"No auth credentials for: {self.url}")
		url = self.url + '/management/client/oauth2/token'
		request = {
			'grant_type': 'password',
			'username': self.user,
			'password': self.password,
			'client_id': self.conf['client']
		}
		try:
			response = requests.post(url, data=request, headers={ 'Content-Type': 'application/x-www-form-urlencoded' })
			response.raise_for_status()
			self.auth = response.json()
			print (f"Connected to {self.url}")
		except requests.exceptions.RequestException as e:
			raise ConnectionError(f"Failed to connect to {self.url}: {e}")
		except json.JSONDecodeError:
			raise ValueError("Failed to decode authentication response.")

	def get_resources(self, criterion=None, options={}):
		print (f"Retrieving projects...")
		url = self.url + '/management/client/get-resources-by-criterion'
		if not criterion:
			criterion = {
				'$or': [
					{'$eq': {'type': 'project'}},
					{'$eq': {'type': 'library'}}
				]
			}
		response = requests.post(url, headers={'Authorization': f"Bearer {self.auth['access_token']}"}, params={}, json={**criterion, **options})
		return response.json() if response.ok else None
		

def execute(**parameters):
	
	bim = BIMcloud(
		parameters.get('user'),
		parameters.get('password'),
		parameters.get('config'))

	res = bim.get_resources()

	return res
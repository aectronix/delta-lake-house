import json
import os
import requests

from datetime import date, datetime, timedelta

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

	def get_traceables(self):
		path = os.path.dirname(os.path.abspath(__file__))
		file_path = os.path.join(path, 'traceables.json')
		if os.path.exists(file_path):
			with open(file_path, 'r') as file:
				return json.load(file)

	def get_last_modified(self):
		traceables = self.get_traceables() or {'updated': '2025-01-01 00:00:00', 'resources': {}}
		from_date = datetime.strptime(traceables['updated'], '%Y-%m-%d %H:%M:%S')
		from_time = from_date.timestamp() * 1000
		criterion = {
			'$and': [
				{'$gte': {'$modifiedDate': from_time }},
				{'$or': [
					{'$eq': {'type': 'project'}},
					{'$eq': {'type': 'library'}}
				]}
			]
		}
		url = self.url + '/management/client/get-resources-by-criterion'
		response = requests.post(url, headers={'Authorization': f"Bearer {self.auth['access_token']}"}, params={}, json={**criterion})
		return response.json() if response.ok else None
	
	def test(self, resources):
		traceables = self.get_traceables() or {'updated': '2025-01-01 00:00:00', 'resources': {}}
		updated = False
		for res in resources:
			# get only modified
			if (
				res['id'] not in resources
				or res['$modifiedDate'] > traceables['updated'].timestamp() * 1000
				or res['$modifiedDate'] > resources[res['id']]['$modifiedDate'].timestamp() * 1000
			):
				updated = True
				friendlyDate = datetime.fromtimestamp(res['$modifiedDate'] / 1000)
				traceables['resources'][res['id']] = {
					'name': res['name'],
					'$modifiedDate': res['$modifiedDate'],
					'@friendlyDate': friendlyDate.strftime("%Y-%m-%d %H:%M:%S")
				}
				print (f"{res['id']}: \"{res['name']}\", {friendlyDate}")

		if updated:
			path = os.path.dirname(os.path.abspath(__file__))
			file_path = os.path.join(path, 'resources.json')
			traceables['updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			with open(file_path, 'w', encoding='utf-8') as file:
				json.dump(traceables, file, ensure_ascii=False, indent=4)

		# TODO: form a dataset with only updated ones

def execute(**parameters):
	
	bim = BIMcloud(
		parameters.get('user'),
		parameters.get('password'),
		parameters.get('config'))

	res = bim.get_last_modified()
	bim.test(res)

	return res
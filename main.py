import argparse
import importlib
import inspect
import json
import os

from typing import Optional, Dict, Any

class LakeHouse():

	def __init__(self):

		self.conf = self._load_config()
		self.scripts = self._get_scripts()

	def _load_config(self, filename='general', scope='config', toObject=True):
		""" Load & process configuration files
		"""
		path = os.path.join(os.path.dirname(__file__), f"{scope}\\{filename}.json")
		if not os.path.exists(path):
			raise FileNotFoundError(f"Configuration file not found: {path}")
		with open(path, 'r') as file:
			config = json.load(file)
		return config

	def _get_scripts(self):
		"""
			Get and register existing scripts.
		"""
		modules = {}
		scripts_dir = os.path.join(os.path.dirname(__file__), 'scripts')

		if not os.path.exists(scripts_dir):
			raise FileNotFoundError(f"Scripts folder not found: {scripts_dir}")

		for folder in os.listdir(scripts_dir):
			folder_path = os.path.join(scripts_dir, folder)
			if not os.path.isdir(folder_path):
				continue
			for file in os.listdir(folder_path):
				if file.endswith('.py') and not file.startswith('__'):
					module_name = f"scripts.{folder}.{file[:-3]}"
					try:
						module = importlib.import_module(module_name)
						modules[file[:-3]] = module  # Store module itself
					except Exception as e:
						print(f"Failed to import {module_name}: {e}")

		return modules

	def execute(self, script: str, function: Optional[str] = 'execute', **parameters: Dict[str, Any]):
		"""
			Run a specific function from the script, pickups related configs.
			Args:
				script (str): The name of the script (module) to run.
				function (str): The specific function to execute (optional).
				**parameters: Additional parameters to pass to the function or class.
			Returns:
				The result of the function execution or the initialized class.
		"""
		if script not in self.scripts:
			raise ImportError(f"No such script found: {script}")
		script_module = self.scripts[script]

		if not hasattr(script_module, function):
			raise AttributeError(f"No such function '{function}' in script {script}")
		f = getattr(script_module, function)

		if not callable(f):
			raise TypeError(f"{function} is not callable in script {script}")

		parameters['config'] = self._load_config(filename=script, scope=f"scripts\\{script}")
		return f(**parameters)


if __name__ == "__main__":

	cmd = argparse.ArgumentParser()
	cmd.add_argument('-s', '--script', required=True, help='Script name to run')
	cmd.add_argument('-u', '--user', required=False, help='User name')
	cmd.add_argument('-p', '--password', required=False, help='User password')
	arg = cmd.parse_args()	

	lake = LakeHouse()
	result = lake.execute(**vars(arg))

	# print (datetime.now())
	# print(json.dumps(result, indent = 4))
	# print(result)
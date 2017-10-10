from __future__ import absolute_import

from setuptools import setup, find_packages

setup (
	name="ing.pipelab",
	version="0.0.1",
	description="Use Dask schedulers to define pipelines",
	packages=find_packages(),
	include_package_data=True,
	install_requires=[
		"dask", "pandas", "joblib", "sqlalchemy",
		"cx_Oracle"
	],
	entry_points="""
		[pipelab.tasks]
		Pipeline=pipelab.task_collection:Pipeline
		Function=pipelab.task_collection:Function
		OracleSQL=pipelab.task_collection:OracleSQL
	"""
)

#!/usr/bin/python

from logging import StreamHandler, INFO, getLogger
from optparse import OptionParser
from os import path
from datetime import datetime
import sqlite3
import boto.glacier.layer2

has_colorlog = True
try:
	from colorlog import ColoredFormatter
except ImportError:
	has_colorlog = False

class SynoGlacier(object):
	def run(self):

		stream = StreamHandler()
		stream.setLevel(INFO)
		if has_colorlog:
			formatter = ColoredFormatter("%(log_color)s%(levelname)-8s%(reset)s %(message)s",
				log_colors={
					'DEBUG':    'white',
					'INFO':     'green',
					'WARNING':  'yellow',
					'ERROR':    'red',
					'CRITICAL': 'red',
				}
			)
			stream.setFormatter(formatter)

		logger = getLogger()
		logger.setLevel(INFO)
		logger.addHandler(stream)
		self.logger = logger

		logger.debug('parsing input')
		parser = OptionParser()

		parser.add_option("-k", "--aws_access_key_id", action="store", type="string", dest="aws_access_key_id",
							help="Your AWS Access-Key")
		parser.add_option("-s", "--aws_secret_access_key", action="store", type="string", dest="aws_secret_access_key",
							help="Your AWS Access-Secret")
		parser.add_option("-r", "--region", action="store", type="string", dest="region", default="us-east-1",
							help="AWS Region Name, one of "+(', '.join(map(lambda r:r.name, boto.glacier.regions()))))

		parser.add_option("-v", "--vault", action="store", type="string", dest="vault",
							help="Glacier-Vault to use. Only required when more then one Synology-DistStation Backup-Jobs write to your Glacier")

		parser.add_option("-d", "--dir", action="store", type="string", dest="dir", default=".",
							help="Target Directory, defaults to current working directory")

		parser.add_option("--port", action="store", type="int", dest="port", default=80,
							help="TCP Port")

		parser.add_option("--proxy", action="store", type="string", dest="proxy",
							help="HTTP-Proxy Hostname")
		parser.add_option("--proxy_port", action="store", type="string", dest="proxy_port",
							help="HTTP-Proxy Port")
		parser.add_option("--proxy_user", action="store", type="string", dest="proxy_user",
							help="HTTP-Proxy Username")
		parser.add_option("--proxy_pass", action="store", type="string", dest="proxy_pass",
							help="HTTP-Proxy Password")

		(options, args) = parser.parse_args()

		logger.info('connecting to glacier')
		layer2 = boto.glacier.layer2.Layer2(
			is_secure = True,
			debug = 1,

			aws_access_key_id = options.aws_access_key_id,
			aws_secret_access_key = options.aws_secret_access_key,
			port = options.port,
			proxy = options.proxy,
			proxy_port = options.proxy_port,
			proxy_user = options.proxy_user,
			proxy_pass = options.proxy_pass,
			region_name = options.region
		)

		logger.info('listing vaults')
		vaults = layer2.list_vaults()
		vaultnames = map(lambda vault:vault.name, vaults)

		if options.vault:
			if vaultnames.count(options.vault) == 0:
				return logger.error('the requested vault %s was not found in this region (%s)', options.vault, options.region)

			if vaultnames.count(options.vault+"_mapping") == 0:
				return logger.error('the mapping-equivalent to your requested vault %s was not found in this region (%s)', options.vault+"_mapping", options.region)

		else:
			dsvaults = []
			for vaultname in vaultnames:
				if vaultnames.count(vaultname+"_mapping") > 0:
					logger.info('identified possible Synology DiskStation Backup: %s', vaultname)
					dsvaults.append(vaultname)
					options.vault = vaultname

			if len(dsvaults) == 0:
				return logger.error('no vault looking like a Synology DiskStation Backup was found. Try to specify it via rhe -v/--vault option')

			if len(dsvaults) > 1:
				return logger.warning('more then one vault looking like a Synology DiskStation Backup was found. Specify which to use via the -v/--vault option')

			options.vault = dsvaults[0]
		
		logger.info('using vault %s', options.vault)

		vault = layer2.get_vault(options.vault)
		mapping_vault = layer2.get_vault(options.vault+"_mapping")

		logger.info('requesting job-listings from vaults')
		jobs = vault.list_jobs()
		mapping_jobs = mapping_vault.list_jobs()

		logger.info('requesting inventory of backup-vault')
		inventory = self.fetch_inventory(vault, jobs)

		logger.info('requesting inventory of mapping-vault')
		#mapping_inventory = self.fetch_inventory(mapping_vault, mapping_jobs)

		if inventory == None or mapping_inventory == None:
			return logger.warn('one of the vaults has not yet finished its inventory task. please wait some more hours until it\'s completed and run this command again')

		if len(mapping_inventory['ArchiveList']) == 0:
			return logger.error('mapping-vault does not contain a archive')

		if len(mapping_inventory['ArchiveList']) > 1:
			logger.warn('mapping-vault does not contains more then one archive, trying to use the first one')

		mapping_archive = mapping_inventory['ArchiveList'][0]

		logger.info('requesting mapping-archive from mapping-vault')
		mapping_archive_data = self.fetch_archive(mapping_vault, mapping_archive, mapping_jobs)

		mapping_filename = path.join(options.dir, '.mapping.sqlite')

		mapping_database = open(mapping_filename, 'wb')
		mapping_database.write(mapping_archive_data)
		mapping_database.close()

		logger.info('successfully fetched mapping database as %s', mapping_filename)

		logger.info('reading mapping database')
		con = sqlite3.connect(mapping_filename)
		cur = con.cursor()

		backup_info = {}
		cur.execute("SELECT key, value FROM backup_info_tb")
		for row in cur:
			backup_info[row[0]] = row[1]



		logger.info('identfied backup as task "%s" from folder "%s" on DiskStation "%s", last run at %s',
			backup_info['taskName'], backup_info['bkpFolder'], backup_info['hostName'], datetime.fromtimestamp(float(backup_info['lastBkpTime'])).isoformat())

		# TODO: allow selection of files to restore

		#cur.execute("SELECT shareName, basePath, archiveID, fileSize FROM file_info_tb")
		#for row in cur:
		#	logger.info




	# List active jobs and check whether any inventory retrieval
	# has been completed, and whether any is in progress. We want
	# to find the latest finished job, or that failing the latest
	# in progress job.
	def fetch_inventory(self, vault, jobs):
		logger = self.logger

		for job in jobs:
			if job.action == "InventoryRetrieval":

				# As soon as a finished inventory job is found, we're done.
				if job.completed:
					logger.info('found finished inventory job: %s', job)
					logger.info('fetching results of finished inventory retrieval')
					
					response = job.get_output()
					inventory = response.copy()
					return inventory

				logger.info('found running inventory job: %s', job)
				logger.info("please wait some more hours until it's completed")
				return None

		logger.info('no inventory jobs finished or running; starting a new job')

		try:
			job = vault.retrieve_inventory_job()
		except boto.glacier.exceptions.UnexpectedHTTPResponseError as e:
			logger.error('failed to create a new inventory retrieval job (#%s: %s)', e.code, e.body)

		return None

	def fetch_archive(self, vault, archive, jobs):
		logger = self.logger

		for job in jobs:
			if job.action == "ArchiveRetrieval" and job.archive_id == archive["ArchiveId"]:

				# As soon as a finished inventory job is found, we're done.
				if job.completed:
					logger.info('found finished retrival job for the requested archive: %s', job)
					logger.info('fetching results of finished retrival (%u bytes)', job.archive_size)
					
					response = job.get_output(validate_checksum=True)
					content = response.read()
					return content

				logger.info('found running retrival job for the requested archive: %s', job)
				logger.info("please wait some more hours until it's completed")
				return None

		logger.info('no retrival job for the requested archive finished or running; starting a new job')

		try:
			job = vault.retrieve_archive(archive["ArchiveId"])
		except boto.glacier.exceptions.UnexpectedHTTPResponseError as e:
			logger.error('failed to create a new archive retrieval job (#%s: %s)', e.code, e.body)

		return None

if __name__ == "__main__":
	SynoGlacier().run()

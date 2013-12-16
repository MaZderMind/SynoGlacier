#!/usr/bin/python

from logging import StreamHandler, INFO, getLogger
from optparse import OptionParser
import os, errno
from os import path
from datetime import datetime
from time import sleep
import sqlite3
import boto.glacier.layer2
import npyscreen

has_colorlog = True
try:
	from colorlog import ColoredFormatter
except ImportError:
	has_colorlog = False

def mkdir_p(path):
	try:
		os.makedirs(path)
	except OSError as exc: # Python >2.5
		if exc.errno == errno.EEXIST and os.path.isdir(path):
			pass
		else: raise

def sizeof_fmt(num):
	for x in ['bytes','KB','MB','GB']:
		if num < 1024.0:
			return "%3.1f %s" % (num, x)
		num /= 1024.0

	return "%3.1f %s" % (num, 'TB')

class SynoGlacier(object):
	def run(self):

		stream = StreamHandler()
		stream.setLevel(INFO)
		if has_colorlog:
			formatter = ColoredFormatter(
				format= "%(log_color)s%(asctime)s %(levelname)-8s%(reset)s %(message)s",
				datefmt="%H:%M:%S",
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

		parser.add_option("-d", "--dir", action="store", type="string", dest="dir", default="./restore/",
							help="Target Directory, defaults to a 'restore'-folder in the current working directory")
		parser.add_option("-o", "--offline", action="store_true", dest="offline",
							help="Use the offline available mapping-database (if available) and avoid the 8h retrieval time")

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

		# none-optional options ;) - I want the arguments to be named
		if not options.aws_access_key_id or not options.aws_secret_access_key or not options.region:
			return logger.error("--aws_access_key_id, --aws_secret_access_key and --region are not optional")

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


		mkdir_p(options.dir)
		mapping_filename = path.join(options.dir, '.mapping.sqlite')

		if options.offline:
			try:
				logger.info('using mapping-archive from offline file')
				with open(mapping_filename, 'rb'):
					pass
			except IOError:
				return logger.error('no offline-mapping-archive found, run without -o/--offline')

		else:
			vault = layer2.get_vault(options.vault)
			mapping_vault = layer2.get_vault(options.vault+"_mapping")

			logger.info('requesting job-listings from mapping-vault')

			logger.info('requesting inventory of mapping-vault')
			mapping_inventory = self.fetch_inventory(mapping_vault)

			if mapping_inventory == None:
				logger.warn('the mapping-vault has not yet finished its inventory task. this script will sleep until it\'s finished and check again every 30 minutes, but you can also cancel it and restart it later')
				while mapping_inventory == None:
					sleep(30*60)
					mapping_inventory = self.fetch_inventory(mapping_vault)

			if len(mapping_inventory['ArchiveList']) == 0:
				return logger.error('mapping-vault does not contain a archive')

			if len(mapping_inventory['ArchiveList']) > 1:
				logger.warn('mapping-vault does not contains more then one archive, trying to use the first one')

			mapping_archive = mapping_inventory['ArchiveList'][0]["ArchiveId"]

			logger.info('requesting mapping-archive from mapping-vault')
			mapping_archive_data = self.fetch_archive(mapping_vault, mapping_archive)

			if mapping_archive_data == None:
				logger.warn('the mapping-archive has not yet finished its retrieval task. this script will sleep until it\'s finished and check again every 30 minutes, but you can also cancel it and restart it later')
				while mapping_archive_data == None:
					sleep(30*60)
					mapping_archive_data = self.fetch_archive(mapping_vault, mapping_archive)

			logger.info('creating target directory (if not existant)')

			with open(mapping_filename, 'wb') as mapping_database:
				mapping_database.write(mapping_archive_data)

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

		cur.execute("SELECT shareName, basePath, archiveID, fileSize FROM file_info_tb")
		dialog = FileSelectionDialog(backup_info = backup_info, file_info = cur.fetchall())

		# TODO: ask for internet download speed
		files = dialog.edit()

		if files == False:
			return logger.error('stopping recovery')

		# TODO: save selected and completed files and offer an option to resume
		# TODO: calculate GBs transferrable per 4h on the selected internet speed

		logger.info(FileSelectionDialog.restoringText % dialog.collectNodeStatistics())
		logger.warn("It will take another 4 hours to start the first retrieval. this script will sleep until then and check again every 30 minutes")


		# TODO: request as many fiels as can fit into the calculated numbet of GBs at a bunch, wait 4h until the first
		#       file is ready, request the next bunch of GBs and while waiting for them, download the previous bunch
		for row in files:
			logger.info("fetching %s (%s)" % (row[1], sizeof_fmt(row[3])));

			restored_file_data = self.fetch_archive(vault, row[2])
			while restored_file_data == None:
				sleep(30*60)
				restored_file_data = self.fetch_archive(vault, row[2])

			restored_filename = path.join(options.dir, row[1])

			mkdir_p(path.dirname(restored_filename))

			with open(restored_filename, 'wb') as restored_file:
				restored_file.write(restored_file_data)

		logger.info("finished restore");
		logger.info("have a nice dataloss-free day");






	# List active jobs and check whether any inventory retrieval
	# has been completed, and whether any is in progress. We want
	# to find the latest finished job, or that failing the latest
	# in progress job.
	def fetch_inventory(self, vault):
		logger = self.logger
		jobs = vault.list_jobs()

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
				return None

		logger.info('no inventory jobs finished or running; starting a new job')

		try:
			job = vault.retrieve_inventory()
		except boto.glacier.exceptions.UnexpectedHTTPResponseError as e:
			logger.error('failed to create a new inventory retrieval job (#%s: %s)', e.code, e.body)

		return None

	def fetch_archive(self, vault, archive):
		logger = self.logger
		jobs = vault.list_jobs()

		for job in jobs:
			if job.action == "ArchiveRetrieval" and job.archive_id == archive:

				# As soon as a finished inventory job is found, we're done.
				if job.completed:
					logger.info('found finished retrival job for the requested archive: %s', job)
					logger.info('fetching results of finished retrival (%s)', sizeof_fmt(job.archive_size))
					
					response = job.get_output(validate_checksum=True)
					content = response.read()
					return content

				logger.info('found running retrival job for the requested archive: %s', job)
				return None

		logger.info('no retrival job for the requested archive finished or running; starting a new job')

		try:
			job = vault.retrieve_archive(archive)
		except boto.glacier.exceptions.UnexpectedHTTPResponseError as e:
			logger.error('failed to create a new archive retrieval job (#%s: %s)', e.code, e.body)

		return None

class FileSelectionDialog(npyscreen.NPSApp):
	formTitle = "Restore Folders"
	statusText = "Currently %u folders with %u files in them (total: %s) are selected for restore"
	restoringText = "Restoring %u folders with %u files in them (total: %s)"

	def __init__(self, backup_info, file_info):
		self.backup_info = backup_info
		self.file_info = file_info

	def edit(self):
		return npyscreen.wrapper_basic(self.show_form)



	def updateText(self):
		self.status.value = FileSelectionDialog.statusText % self.collectNodeStatistics()
		self.status.display()
		self.tree._display()
		return

	def collectNodeStatistics(self):
		selectedNodes = self.tree.get_selected_objects(return_node=True)
		dcnt = fcnt = szsum = 0

		files = []
		for node in selectedNodes:
			dcnt += 1
			if hasattr(node, 'files'):
				fcnt += len(node.files)
				files.extend(node.files)
				for file in node.files:
					try:
						szsum += file[3]
					except TypeError:
						pass

		return (dcnt, fcnt, sizeof_fmt(szsum))


	def on_ok(self):
		return True

	def show_form(self, *args):
		form = npyscreen.ActionForm(name=FileSelectionDialog.formTitle)
		status = form.add(npyscreen.FixedText, value=FileSelectionDialog.statusText % (0, 0, 0), editable=False)
		tree = form.add(npyscreen.MLTreeMultiSelect, rely=4)

		tree._display = tree.display
		tree.display = self.updateText

		self.status = status
		self.tree = tree
		
		treedata = npyscreen.NPSTreeData(content=self.backup_info['bkpFolder']+' on '+self.backup_info['hostName'], ignoreRoot=False)
		self.build_treedata('', treedata)
		tree.values = treedata

		form.on_ok = self.on_ok
		if not form.edit():
			return False

		nodes = tree.get_selected_objects(return_node=True)
		files = []
		for node in nodes:
			if hasattr(node, 'files'):
				files.extend(node.files)

		return files


	def build_treedata(self, prefix, parent):
		#self.logger.info('entering build_treedata() with prefix="%s" and parent="%s"', prefix, parent.getContentForDisplay())

		handled_folders = []
		parent.files = []

		# iterate over all rows
		for row in self.file_info:
			# test if the row begins with the desired prefix
			if row[1].find(prefix) == 0:
				# test if the row specifies a file below that predfix
				subpath = row[1][len(prefix):]

				# it is a file
				if subpath.find('/') == -1:
					# TODO: count up somehow
					parent.files.append(row)
					#parent.newChild(content="FILE "+subpath)

				# it is a folder
				else:
					# test if we already had that folder
					# generate the folder-name from the subpath
					dirname = subpath[:subpath.find('/')]
					dirpath = prefix+dirname+"/"

					#self.logger.info('   path="%s", subpath="%s", dirname="%s", dirpath="%s"', row[1], subpath, dirname, dirpath)

					if handled_folders.count(dirpath) > 0:
						continue

					# remember that we already had this folder
					handled_folders.append(dirpath)

					# add a node for that folder to the tree
					node = parent.newChild(content=dirname.encode('UTF-8'))

					# recurse for files below that folder
					self.build_treedata(dirpath, node)


		parent.setContent(parent.getContent() + " (%u Files)" % len(parent.files))




if __name__ == "__main__":
	SynoGlacier().run()

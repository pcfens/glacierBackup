import argparse
import tarfile
import tempfile
import os
import sys
from time import gmtime, strftime

# Use Paul Engstler's Glacier Library - http://paulengstler.github.com/glacier/
import glacier

parser = argparse.ArgumentParser(description="Backup folders to Amazon Glacier")
parser.add_argument("paths", metavar="path", nargs='*', 
                    help="The path of what you want to backup or restore to as a compressed archive (can be repeated if backing up)")

parser.add_argument('-k', '--key', required=True,
                    help="Your Amazon Access Key")
parser.add_argument('-s', '--secret', required=True,
                    help="Your Amazon Secret Access Key")

vaultOptions = parser.add_mutually_exclusive_group(required=True)
vaultOptions.add_argument('-v', '--vault',
                    help="The vault where you want to store your archives.  If the vault does not exist it will be created.")
vaultOptions.add_argument('-l', '--list-vaults', action='store_true',
                    help="List the vaults available with the supplied key")

actionItem = parser.add_mutually_exclusive_group()
actionItem.add_argument('-j', '--job',
                    help='The job ID of what you\'re checking')
actionItem.add_argument('-a', '--archive',
                    help="The archive to queue for retrieval (or delete if -d/--delete is set)")

actions = parser.add_mutually_exclusive_group()
actions.add_argument('-i', '--get-inventory', action='store_true',
                    help="Initiate a job to retrieve the inventory of the specified vault")
actions.add_argument('-f', '--fetch', action='store_true',
                    help="Fetch the output of the specified job ID after the retrieval job finishes")
actions.add_argument('-d', '--delete', action='store_true',
                    help="Delete the given archive contained in the specified vault")

parser.add_argument('-t', '--test', action='store_true',
                    help="Don't actually do anything - just print what would happen (not fully implemented yet)")

args = parser.parse_args()

if not (args.vault or args.list_vaults):
    parser.error("Either --vault or --list-vaults must be given")

tempDir = tempfile.gettempdir()

if args.test:
    print "Connect to AWS"
    print "Fetch list of vaults"
    print args
else:
    connection = glacier.Connection(args.key, args.secret)
    vaults = connection.get_all_vaults();
    
    if args.list_vaults:
        for vault_i in vaults:
            print vault_i.name
        sys.exit()
    
    if vaults.count(args.vault) != 0:
        vault = connection.get_vault(args.vault)
    else:
        vault = connection.create_vault(args.vault)
        
    if args.get_inventory:
        if args.test:
            print "Initiate a job to retrieve the vault inventory"
            sys.exit()
        else:
            if args.job:
                for archive in vault.get_job_output(args.job)['ArchiveList']:
                    print archive['ArchiveId'] + "\t" + archive['ArchiveDescription']
                sys.exit()
            else:
                job_id = vault.initiate_job("inventory-retrieval")
                print "The job ID of the retrieval job is " + job_id
                sys.exit()
 
    if args.archive:
        archive = glacier.Archive(args.archive)
        if args.delete:
            job_id = vault.delete(archive)
            sys.exit()
        job_id = vault.initiate_job("archive-retrieval", archive=archive)
        print "The archive retrieval job is " + job_id
        sys.exit()
            
    if args.job:
        if args.fetch:
            if len(args.paths) != 1:
                raise Exception("Exactly one out file path is required to fetch a file")
            archiveFile = vault.get_job_output(args.job, output='raw')
            outFile = open(args.paths[0], 'w')
            outFile.write(archiveFile)
            outFile.close()
        else:
            print vault.get_job_output(args.job)
            
        sys.exit()

for path in args.paths:
    absDirName = os.path.abspath(os.path.normpath(path))
    dirName = os.path.basename(absDirName)
    archiveName = tempDir + "/" + dirName + ".tar.bz2"
    
    if args.test:
        print "Store " + absDirName + " in " + archiveName
    else:
        archive = tarfile.open(archiveName, mode="w:bz2")
        print "Archiving " + absDirName
        archive.add(os.path.abspath(os.path.normpath(path)), arcname=dirName)
        archive.close()
        print "Archived " + absDirName
        
    if args.test:
        print "Upload " + archiveName + " to Glacier vault " + args.vault
        print "Remove " + archiveName + " from local system"
    else:
        archive = glacier.Archive(archiveName)
        print "Uploading " + archiveName
        vault.upload(archive, "Tarball of " + absDirName + " taken at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
        print archiveName + " uploaded with ID " + archive.id
        os.remove(archiveName)

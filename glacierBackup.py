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
                    help="The path of what you want to backup as a compressed archive (can be repeated)")
parser.add_argument('-t', '--test', action='store_true',
                    help="Don't actually do anything - just print what would happen.")
parser.add_argument('-k', '--key', required=True,
                    help="Your Amazon Access Key")
parser.add_argument('-s', '--secret', required=True,
                    help="Your Amazon Secret Access Key")
parser.add_argument('-v', '--vault',
                    help="The vault where you want to store your archives.  If the vault does not exist it will be created.")
parser.add_argument('-l', '--list-vaults', action='store_true',
                    help="List the vaults available with the supplied key")
args = parser.parse_args()

if not (args.vault or args.list_vaults):
    parser.error("Either --vault or --list-vaults must be given")

tempDir = tempfile.gettempdir()

if args.test:
    print "Connect to AWS"
    print "Fetch list of vaults"
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

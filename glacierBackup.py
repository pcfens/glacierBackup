import argparse
import tarfile
import tempfile
import os
from time import gmtime, strftime

# Use Paul Engstler's Glacier Library - http://paulengstler.github.com/glacier/
import glacier

parser = argparse.ArgumentParser(description="Backup folders to Amazon Glacier")
parser.add_argument("paths", metavar="path", nargs='+', 
                    help="The path of what you want to backup as a compressed archive (can be repeated)")
parser.add_argument('-t', '--test', action='store_true',
                    help="Don't actually do anything - just print what would happen.")
parser.add_argument('-k', '--key', required=True,
                    help="Your Amazon Access Key")
parser.add_argument('-s', '--secret', required=True,
                    help="Your Amazon Secret Access Key")
parser.add_argument('-v', '--vault', required=True,
                    help="The vault where you want to store your archives.  If the vault does not exist it will be created.")
args = parser.parse_args()
tempDir = tempfile.gettempdir()

if args.test:
    print "Connect to AWS"
    print "Fetch list of vaults"
    print "If " + args.vault + " does not exist create it."
else:
    connection = glacier.Connection(args.key, args.secret)
    vaults = connection.get_all_vaults();
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
        archive.add(os.path.abspath(os.path.normpath(path)), arcname=dirName)
        archive.close()
        
    if args.test:
        print "Upload " + archiveName + " to Glacier vault " + args.vault
        print "Remove " + archiveName + " from local system"
    else:
        archive = glacier.Archive(archiveName)
        vault.upload(archive, "Tarball of " + absDirName + " taken at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
        print archiveName + " uploaded with ID " + archive.id
        os.remove(archiveName)

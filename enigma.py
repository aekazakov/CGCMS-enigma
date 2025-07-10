import requests
import logging
import os
import urllib
import shutil
from django.utils import timezone
from django.utils import dateformat
from collections import defaultdict
from pathlib import Path
from browser.models import Config
from browser.models import Genome


logger = logging.getLogger("GenomeDepot")


def download_enigma_strain_metadata():
    # Download from isolates.genomics.lbl.gov 
    metadata_imported = defaultdict(dict)
    return metadata_imported
    logger.info('Downloading metadata from isolates.genomics.lbl.gov')
    url = 'http://isolates.genomics.lbl.gov/api/v1/isolates/id/'
    external_url = 'http://isolates.genomics.lbl.gov/isolates/id/'
    fields = [('condition',
               'Isolation conditions/description (including temperature)'
               ),
              ('order', 'Phylogenetic Order'),
              ('closest_relative',
                'Closest relative in NCBI: 16S rRNA Gene Database'
                ),
              ('similarity', 'Similarity (%)'),
              ('date_sampled', 'Date sampled'),
              ('sample_id', 'Well/Sample ID'),
              ('lab', 'Lab isolated/Contact'),
              ('campaign', 'Campaign or Set'),
              ('rrna', 'rrna')
              ]
    error_threshold = 10
    isolate_max_id = 100000
    errors = 0
    for isolate_id in range(isolate_max_id):
        isolate_url = url + str(isolate_id)
        r = requests.get(url=isolate_url)
        # extracting data in json format 
        data = r.json()
        if 'id' not in data:
            logger.warning('%s returned no data', isolate_url)
            errors += 1
        if errors >= error_threshold:
            logger.info('Download stopped after %d errors. Last url is %s',
                        error_threshold,
                        isolate_url
                        )
            break
        try:
            logger.info(str(data['id']) + ' ' + data['isolate_id'])
            strain_id = data['isolate_id']
            for field in fields:
                if field[0] in data:
                    if data[field[0]] is not None and data[field[0]] != '':
                        metadata_imported[strain_id][field[1]] = \
                            ('ENIGMA Isolate Browser',
                             external_url + str(isolate_id),
                             str(data[field[0]])
                             )
        except KeyError:
            continue

def check_enigma_repository(username, password):
    timestamp = dateformat.format(timezone.localtime(timezone.now()),'Ymd',)
    config = {}
    ret = []
    for item in Config.objects.values('param', 'value'):
        config[item['param']] = item['value']

    download_dir = os.path.join(config['core.temp_dir'], 'genomes_update_' + timestamp)
    Path(download_dir).mkdir(exist_ok=True, parents=True)

    #importer = Importer()
    existing_genomes = defaultdict(dict)
    for genome_data in Genome.objects.values_list('name', 'strain__strain_id', 'external_url', 'external_id'):
        if '#dataview' in genome_data[2]:
            kbase_id = genome_data[2].split('#dataview/')[1]
        else:
            kbase_id = ''
        existing_genomes[genome_data[1]][genome_data[0]] = kbase_id
        print(genome_data)

    #This should be the base url you wanted to access.
    baseurl = 'https://genomics.lbl.gov/enigma-data/genome_processing'

    #Create a password manager
    manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    manager.add_password(None, baseurl, username, password)

    #Create an authentication handler using the password manager
    auth = urllib.request.HTTPBasicAuthHandler(manager)

    #Create an opener that will replace the default urlopen method on further calls
    opener = urllib.request.build_opener(auth)
    urllib.request.install_opener(opener)

    #Here you should access the full url you wanted to open
    response = urllib.request.urlopen(baseurl + "/manifest.tsv")
    data = response.read()
    lines = data.decode('utf-8').split('\n')
    print(lines[0])
    output_table = []

    for line in lines[1:]:
        row = line.split('\t')
        if len(row) > 1 and row[2] == 'Genome':
            strain_id = row[0]
            genome_id = row[3].split('/')[-2]
            gbff_url = baseurl + '/' + row[3]
            if row[5].startswith('KBase'):
                kbase_id = row[5].split(': ')[-1]
            else:
                kbase_id = row[5]
            gbff_file= os.path.join(download_dir,genome_id + '.gbff')
            if strain_id in existing_genomes:
                if genome_id in existing_genomes[strain_id]:
                    print('Genome found', genome_id, 'of', strain_id)
                    #pass
                else:
                    kbase_found = False
                    for genome in existing_genomes[strain_id].keys():
                        if existing_genomes[strain_id][genome] == kbase_id:
                            kbase_found = True
                            break
                        else:
                            print('NO MATCH', kbase_id, existing_genomes[strain_id][genome])
                    if not kbase_found:
                        print('New genome', genome_id)
                        if gbff_url.endswith('.gbff'):
                            output_table.append([gbff_file, genome_id, strain_id, '', gbff_url, row[5]])
                        
            else:
                if gbff_url.endswith('.gbff'):
                    output_table.append([gbff_file, genome_id, strain_id, '', gbff_url, row[5]])
                else:
                    ret.append('WRONG FILE EXTENSION: ' + gbff_url)
                    
    if not output_table:
        ret.append('New genomes not found')
        return '\n'.join(ret), 'No new genomes in the ENIGMA data repository'

    print('Downloading', len(output_table), 'genomes')
    genome_count = 0
    with open(os.path.join(download_dir, 'enigma_genomes_update_' + timestamp + '.txt'), 'w') as out_log:
        out_log.write('#gbk_file\tGenome\tStrain\tSample\tURL\tExternalID\n')
        for row in output_table:
            try:
                if os.path.exists(row[0]):
                    continue
                with urllib.request.urlopen(row[4]) as response, open(row[0], 'wb') as out_file:
                    shutil.copyfileobj(response, out_file)
                out_log.write('\t'.join(row) + '\n')
                genome_count += 1
            except Exception as e:
                ret.append(row[4] + ' download error. File ' + row[0] + ' was not created.')
                ret.append(f"An error occurred: {e}")
                continue
            #break
    ret.insert(0, "New genomes: " + str(len(output_table)))
    ret.insert(0, f"New genomes downloaded: {genome_count}")
    return '\n'.join(ret), 'NEW GENOMES found in the ENIGMA data repository!'
        

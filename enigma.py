import requests
from collections import defaultdict
import logging

logger = logging.getLogger("CGCMS")


def download_enigma_strain_metadata():
    # Download from isolates.genomics.lbl.gov 
    metadata_imported = defaultdict(dict)
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
            logger.info(data['id'] + ' ' + data['isolate_id'])
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

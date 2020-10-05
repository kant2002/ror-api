import base32_crockford
import json
import os.path
import random
import zipfile
from rorapi.settings import ES, ES_VARS, ROR_API, GRID, ROR_DUMP

from django.core.management.base import BaseCommand


def generate_ror_id():
    """Generates random ROR ID.

    The checksum calculation is copied from
    https://github.com/datacite/base32-url/blob/master/lib/base32/url.rb
    to maintain the compatibility with previously generated ROR IDs.
    """

    n = random.randint(0, 200000000)
    n_encoded = base32_crockford.encode(n).lower().zfill(6)
    checksum = str(98 - ((n * 100) % 97)).zfill(2)
    return '{}0{}{}'.format(ROR_API['ID_PREFIX'], n_encoded, checksum)

def ror_exists(grid_id, es):
    """Maps GRID ID to ROR ID.

    If given GRID ID was indexed previously, corresponding ROR ID is obtained
    from the index. Otherwise, new ROR ID is generated.
    """

    s = ES.search(ES_VARS['INDEX'],
              body={'query': {
                  'term': {
                      'external_ids.GRID.all': grid_id
                  }
              }})
    if s['hits']['total'] == 1:
        return s['hits']['hits'][0]['_id']

def get_ror_id(grid_id, es):
    return ror_exists(grid_id, es) if ror_exists(grid_id, es) else generate_ror_id()

def active_record (grid_org,es):
    return {
        'id':
        get_ror_id(grid_org['id'], ES),
        'name':
        grid_org['name'],
        'types':
        grid_org['types'],
        'links':
        grid_org['links'],
        'aliases':
        grid_org['aliases'],
        'acronyms':
        grid_org['acronyms'],
        'status':
        grid_org['status'],
        'wikipedia_url':
        grid_org['wikipedia_url'],
        'labels':
        grid_org.get('labels',[]),
        'country': {
            'country_code': grid_org['addresses'][0]['country_code'],
            'country_name': grid_org['addresses'][0]['country']
            },
            'external_ids':
            getExternalIds(
            dict(grid_org.get('external_ids', {}),
                GRID={
                    'preferred': grid_org['id'],
                    'all': grid_org['id']
                    }))
        }

def obsolete_record(grid_org, es):
    return {
        'id':
        ror_exists(grid_org['id'],es) if ror_exists(grid_org['id'],es) else grid_org['id'],
        'status':'obsolete',
        'name': '',
        'external_ids':
        getExternalIds(
        dict(grid_org.get('external_ids', {}),
            GRID={
                'preferred': grid_org['id'],
                'all': grid_org['id']
                }))
    }
def redirect_record(grid_org, es, active_data):
    # get ROR id of the item this is redirecting to
    # using the hash instead of querying again because it is unknown whether the organization
    # being redirected to is already present in ES or is a new organization about to be indexed

    #adding grid_redirect key in case the historical organization being redirected to, doesn't exist later
    redirect_id = next(item for item in active_data if item['external_ids']['GRID']['preferred'] == grid_org['redirect'])['id']
    return {
        'id':
        ror_exists(grid_org['id'],es) if ror_exists(grid_org['id'],es) else grid_org['id'],
        'name':"",
        'status':'redirected',
        'redirect_to':redirect_id,
        'grid_id_redirect': grid_org['redirect'],
        'external_ids':
        getExternalIds(
        dict(grid_org.get('external_ids', {}),
            GRID={
                'preferred': grid_org['id'],
                'all': grid_org['id']
                }))
    }

def convert_organization(grid_org, es, active_data = None):
    """Converts the organization metadata from GRID schema to ROR schema."""
    if grid_org['status'] == 'active':
        return active_record(grid_org,es)
    elif grid_org['status'] == 'obsolete':
        return obsolete_record(grid_org,es)
    elif grid_org['status'] == 'redirected':
       return redirect_record(grid_org, es, active_data)


def getExternalIds(external_ids):
    if 'ROR' in external_ids: del external_ids['ROR']
    return external_ids


class Command(BaseCommand):
    help = 'Converts GRID dataset to ROR schema'

    def handle(self, *args, **options):
        os.makedirs(ROR_DUMP['DIR'], exist_ok=True)
        # make sure we are not overwriting an existing ROR JSON file
        # with new ROR identifiers
        if zipfile.is_zipfile(ROR_DUMP['ROR_ZIP_PATH']):
            self.stdout.write('ROR dataset already exist')
            return

        if not os.path.isfile(ROR_DUMP['ROR_JSON_PATH']):
            with open(GRID['GRID_JSON_PATH'], 'r') as it:
                grid_data = json.load(it)

            self.stdout.write('Converting GRID dataset to ROR schema')
            active_ror_data = [
                convert_organization(org, ES)
                for org in grid_data['institutes'] if org['status'] == 'active'
            ]
            inactive_ror_data = [
                convert_organization(org, ES, active_ror_data)
                for org in grid_data['institutes'] if org['status'] != 'active'
            ]
            ror_data = active_ror_data + inactive_ror_data
            with open(ROR_DUMP['ROR_JSON_PATH'], 'w') as outfile:
                json.dump(ror_data, outfile, indent=4)
            self.stdout.write('ROR dataset created')

        # generate zip archive
        with zipfile.ZipFile(ROR_DUMP['ROR_ZIP_PATH'], 'w') as zipArchive:
            zipArchive.write(ROR_DUMP['ROR_JSON_PATH'],
                             arcname='ror.json',
                             compress_type=zipfile.ZIP_DEFLATED)
            self.stdout.write('ROR dataset ZIP archive created')

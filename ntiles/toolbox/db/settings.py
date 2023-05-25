"""
Define Global Settings
"""

DB_CONNECTION_STRING = '/Users/alex/Desktop/DB/wrds.duckdb'  # the directory to the sql database
CACHE_DIRECTORY = '/tmp'  # the directory to cache files, QueryConstructor gets cached here
ETF_UNI_DIRECTORY = '/tmp'  # '/Users/alex/Desktop/DB/universes/etf'  # the directory to save ETF Universes
BUILT_UNI_DIRECTORY = '/Users/alex/Desktop/DB/universes/built'  # directory to save custom-built universes

DB_ADJUSTOR_FIELDS = {
    'cstat.sd': [
        {
            'adjustor': 'ajexdi',
            'fields_to_adjust': ['prccd', 'prcod', 'prchd', 'prcld', 'eps'],
            'operation': '/'
        },
        {
            'adjustor': 'ajexdi',
            'fields_to_adjust': ['cshoc', 'cshtrd'],
            'operation': '*'
        }
    ],
    'crsp.sd': [
        {
            'adjustor': 'cfacpr',
            'fields_to_adjust': ['prc', 'openprc', 'askhi', 'bidlo', 'bid', 'ask'],
            'operation': '/',
            'function': 'ABS'
        },
        {
            'adjustor': 'cfacshr',
            'fields_to_adjust': ['vol', 'shrout'],
            'operation': '*'
        }

    ],
    'crsp.sm': [
        {
            'adjustor': 'cfacpr',
            'fields_to_adjust': ['prc', 'openprc', 'askhi', 'bidlo', 'bid', 'ask', 'altprc'],
            'operation': '/',
            'function': 'ABS'
        },
        {
            'adjustor': 'cfacshr',
            'fields_to_adjust': ['vol', 'shrout'],
            'operation': '*'
        }

    ],
    'cstat.sm': [
        {
            'adjustor': 'ajexm',
            'fields_to_adjust': ['prccm', 'prchm', 'prclm'],
            'operation': '/'
        },
        {
            'adjustor': 'ajexm',
            'fields_to_adjust': ['cshom', 'cshtrm'],
            'operation': '*'
        }
    ],
    'cstat.funda': [
        {'fields_to_adjust': []}
    ],

    'wrds.firm_ratios': [
        {'fields_to_adjust': []}
    ],
    'ibes.summary_price_target': [
        {'fields_to_adjust': []}
    ]
}

DB_ADJUSTOR_FIELDS['sd'] = DB_ADJUSTOR_FIELDS['cstat.sd']
DB_ADJUSTOR_FIELDS['main.sd'] = DB_ADJUSTOR_FIELDS['cstat.sd']

# sql code to link permno to cstat and ibes
ADD_ALL_LINKS_TO_PERMNO = """
(
    SELECT --columns 
    FROM
        --from LEFT JOIN link.crsp_cstat_link AS ccm ON (uni.permno = ccm.lpermno AND uni.date >= ccm.linkdt 
                AND uni.date <= ccm.linkenddt AND (ccm.linktype = 'LU' OR ccm.linktype = 'LC'))
        LEFT JOIN link.crsp_ibes_link AS crib ON (uni.permno = crib.permno AND uni.date >= crib.sdate 
            AND uni.date <= crib.edate)
)
"""

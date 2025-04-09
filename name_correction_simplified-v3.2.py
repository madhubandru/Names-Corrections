"""
Enhanced Name Correction System - Simplified Interface
--------------------------------------------
This module provides a simplified interface for the enhanced name correction system
for matching wholesaler agent names against a standard distribution list.

Improvements:
- Prioritizes exact matches in the distribution list
- Incorporates company name matching for better accuracy
- Adjusts confidence levels to only show high, medium, or no match
- Preserves original names when no good match is found

Simply run this script and follow the prompts to input file paths and column names.
"""

"""
Enhanced Name Correction System with Company Mapping
--------------------------------------------
This module provides a comprehensive name correction system with company mapping
for matching wholesaler agent names against a standard distribution list.

Improvements:
- Added company mapping dictionary for standardizing company names
- Enhanced name swap detection for cases where first and last names are switched
- Improved initial matching for cases with partial names
- Adjusted string handling to prevent errors with non-string inputs
"""

import pandas as pd
import numpy as np
import re
from collections import defaultdict
import jellyfish
from rapidfuzz import fuzz, process
import time
import pickle
import os
import logging
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("name_correction.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("name_correction")

# Common nickname mappings
NICKNAME_MAP = {
'abby': 'abigail',
'al': 'albert',
'alex': 'alexander',
'andy': 'andrew',
'ben': 'benjamin',
'benny': 'benjamin',
'beth': 'elizabeth',
'betty': 'elizabeth',
'bill': 'william',
'bob': 'robert',
'bobby': 'robert',
'cathy': 'catherine',
'charlie': 'charles',
'chris': 'christopher',
'chuck': 'charles',
'dan': 'daniel',
'danny': 'daniel',
'dave': 'david',
'deb': 'deborah',
'debbie': 'deborah',
'dick': 'richard',
'don': 'donald',
'donny': 'donald',
'drew': 'andrew',
'ed': 'edward',
'eddie': 'edward',
'fred': 'frederick',
'freddy': 'frederick',
'gail': 'abigail',
'greg': 'gregory',
'harry': 'harold',
'jack': 'john',
'jackie': 'john',
'jeff': 'jeffrey',
'jen': 'jennifer',
'jenny': 'jennifer',
'jess': 'jessica',
'jessie': 'jessica',
'jim': 'james',
'jimmy': 'james',
'joe': 'joseph',
'joey': 'joseph',
'kate': 'katherine',
'kathy': 'katherine',
'ken': 'kenneth',
'kenny': 'kenneth',
'kim': 'kimberly',
'larry': 'lawrence',
'liz': 'elizabeth',
'maggie': 'margaret',
'matt': 'matthew',
'meg': 'margaret',
'mike': 'michael',
'nancy': 'ann',
'nate': 'nathan',
'nick': 'nicholas',
'pam': 'pamela',
'pat': 'patricia',
'patty': 'patricia',
'peggy': 'margaret',
'ray': 'raymond',
'rich': 'richard',
'rick': 'richard',
'rob': 'robert',
'ron': 'ronald',
'ronny': 'ronald',
'sam': 'samuel',
'sammy': 'samuel',
'steve': 'stephen',
'sue': 'susan',
'suzy': 'susan',
'ted': 'theodore',
'tim': 'timothy',
'timmy': 'timothy',
'tom': 'thomas',
'tommy': 'thomas',
'tony': 'anthony',
'val': 'valerie',
'vic': 'victor',
'vicky': 'victoria',
'vince': 'vincent',
'walt': 'walter',
'will': 'william',
'willy': 'william'
}

# Company mapping dictionary
COMPANY_MAP = {
    # Common abbreviations and variations

'5th 3rd Bank Downtown Branch':'5/3rd',
'Fifth Third':'5/3rd',
'fifth thrid':'5/3rd',
'Advisor':'AAG-?',
'All State':'Allstate',
'Allatate':'Allstate',
'Allstate Efs':'Allstate',
'Allstate Financial':'Allstate',
'Allstate Financial Serv':'Allstate',
'Allstate Financial Serv LLC':'Allstate',
'Allstate Financial Services':'Allstate',
'Allstate Insurance Company':'Allstate',
'kgoza@allstate.com':'Allstate',
'American Capitol Partners':'American Capital Partners',
'American Century Funds':'American Century',
'American Port':'American portfolios',
'American Portfolios Fin Serv':'American portfolios',
'amerias':'Ameritas',
'Ameritas Investment Company':'Ameritas',
'Arvest Wealth Management':'Arvest',
'ASBO Colorado':'ASBO',
'Indiana ASBO':'ASBO Indiana',
'ASBO Int\'l':'ASBO International',
'Aspire/PCS':'Aspire',
'Auditor of State of Ohiio':'Auditor of State of Ohio',
'Avantax - AL':'Avantax',
'Avantax Investment Services':'Avantax',
'Avantax Investment Svcs, Inc.':'Avantax',
'Avantax-AL':'Avantax',
'b riley wealth':'B Riley',
'B. Riley':'B Riley',
'B. Riley Wealth Mgt':'B Riley',
'B.Riley Wealth':'B Riley',
'Banker':'Bankers Life',
'Banker Life':'Bankers Life',
'Bankers':'Bankers Life',
'Ben F Edwards':'Benjamin F Edwards',
'Benjamin Edwards':'Benjamin F Edwards',
'Benjamin F. Edwards':'Benjamin F Edwards',
'Benjamin Wdwards':'Benjamin F Edwards',
'BYN Mellon':'BNY Mellon',
'Bossier Sheriff Dep':'Bossier Sheriff Dept',
'Brokers':'Brokers International',
'Brokers Int\'l':'Brokers International',
'Brokers Intl Fin Services':'Brokers International',
'Brokers Intl Financial':'Brokers International',
'Cadaret':'Cadaret & Grant',
'Cadaret Grant':'Cadaret & Grant',
'Caderet Grant':'Cadaret & Grant',
'Cadret Grant':'Cadaret & Grant',
'calton and associates':'Calton',
'Cambridge - MS':'Cambridge',
'Cambridge Investment':'Cambridge',
'Cambridge investment research':'Cambridge',
'Cambridge-AL':'Cambridge',
'Cambridge-MS':'Cambridge',
'Cambridge Invest':'Cambridge Investment',
'Cambridge Investment Research':'Cambridge Investment',
'Cambridge Investments':'Cambridge Investment',
'Cape sec':'Cape Securities',
'Cape Securites':'Cape Securities',
'Capital Investment Group Inc.':'Capital Investment Group',
'Capital Investment Group, Inc':'Capital Investment Group',
'Capitol Securities Mgmt Inc.':'Capitol Securities',
'CASBO Executive Director':'CASBO',
'Centarus':'Centaurus',
'Centaurus Financial':'Centaurus',
'Centaurus Financial,  Inc':'Centaurus',
'Centaurus Financial, Inc':'Centaurus',
'Centerarus':'Centaurus',
'Centuraus':'Centaurus',
'Cetera':'Cetara',
'Cetera - AL':'Cetara',
'Cetera Advisor Network':'Cetara',
'Cetera Advisor Networks':'Cetara',
'Cetera Advisor Networks, LLC':'Cetara',
'Cetera Advisors':'Cetara',
'Cetera Advisors, LLC':'Cetara',
'Cetera Financial':'Cetara',
'Cetera Financial Services':'Cetara',
'Cetera Investment Services':'Cetara',
'Cetera Investment Services LLC':'Cetara',
'Cetera Investor Services':'Cetara',
'Cetera Wealth Partners':'Cetara',
'Cetera/Regions':'Cetara',
'Cetera/Regions-AL':'Cetara',
'Cetera-AL':'Cetara',
'Cetera-GA':'Cetara',
'Cetera Advisor Network':'Cetera Advisor Networks',
'Cetera Advisor Networks, LLC':'Cetera Advisor Networks',
'Cetera advisors network':'Cetera Advisor Networks',
'Cetera Financial':'Cetera Financial Group',
'Cetera Investment Services':'Cetera Investment Services,LLC',
'Cetera Investment Services LLC':'Cetera Investment Services,LLC',
'Cetera Investment Services,LLC Cetera Investment Services,LLC':'Cetera Investment Services,LLC',
'Cetera investments':'Cetera Investment Services,LLC',
'Cetrea Investors':'Cetera Investors',
'Cetera Wealth':'Cetera Wealth Partners',
'Cetera WP':'Cetera Wealth Partners',
'Regions, Cetera':'Cetera/Regions',
'Regions/Cetera':'Cetera/Regions',
'regions-cetera':'Cetera/Regions',
'Cetera - AL':'Cetera/Regions-AL',
'Cetera/Regions - AL':'Cetera/Regions-AL',
'Cetera-AL':'Cetera/Regions-AL',
'CFD':'CFD Investments',
'Chairvolotti Financial LPL':'Chairvolotti Financial',
'Chelsea':'Chelsea Financial Service',
'Chelsea Financial':'Chelsea Financial Service',
'Cit':'Citbank',
'Citi':'Citbank',
'Citi Bank':'Citbank',
'Citibank':'Citbank',
'citigroup':'Citbank',
'Citi':'Citi Bank',
'City Hope':'City Hope Church',
'Client':'ClientOne',
'Client One':'ClientOne',
'Comminwealth':'Commonwealth',
'Common Wealth':'Commonwealth',
'Commonwealth Financial':'Commonwealth',
'Commonwealth Financial Network':'Commonwealth',
'Commonwelath':'Commonwealth',
'Commenwealth Fin':'Commonwealth Financial Network',
'Commonwealth financial':'Commonwealth Financial Network',
'Commonwealth Investment Services':'Commonwealth Investments',
'Concorde':'Concorde Investments',
'Concourse':'Concourse Fin Group Securities',
'concourse fin group':'Concourse Fin Group Securities',
'Concourse Home Office':'Concourse Fin Group Securities',
'CoreCap':'Corecap Investments',
'Corrado Advisors':'Corrado Financial',
'Crown capital corp':'Crown Capital',
'Crown Capital Securities':'Crown Capital',
'CUSO':'CUSO Financial Services, L.P.',
'Cuso Financial':'CUSO Financial Services, L.P.',
'CUSO Financial Services':'CUSO Financial Services, L.P.',
'CUSO Financial Services,L.P.':'CUSO Financial Services, L.P.',
'CUSO/Wescom':'CUSO Financial Services, L.P.',
'Wescom-CUSO':'CUSO/Wescom',
'D.H. Hill':'D H Hill securities',
'Dempsey':'Dempsey Lord Smith',
'Dempsey L':'Dempsey Lord Smith',
'Dempsey Lord and Smith':'Dempsey Lord Smith',
'Dempsey, Lord and Smith':'Dempsey Lord Smith',
'DFPG':'DFPG Investments',
'DFPG Investments, Inc.':'DFPG Investments',
'DPL Financial':'DPL',
'DPL Financial Partners':'DPL',
'1290':'EQH',
'1290 Funds':'EQH',
'AXA':'EQH',
'Axa 401k':'EQH',
'AXA Advisor':'EQH',
'AXA Advisors':'EQH',
'AXA Advisors LLC':'EQH',
'AXA Advisors, LLC':'EQH',
'AXA Distributors':'EQH',
'AXA Equitable':'EQH',
'AXA Equitable Advisors':'EQH',
'Axa VA':'EQH',
'EQ':'EQH',
'EQ Advisor':'EQH',
'EQ Advisors':'EQH',
'EQ Advosors':'EQH',
'EQA':'EQH',
'EQH Adv':'EQH',
'EQH Advisor':'EQH',
'EQH Advisors':'EQH',
'EQH Advisors LLC':'EQH',
'EQH Asvisors':'EQH',
'EQH Dist':'EQH',
'EQH GR':'EQH',
'EQH Internal':'EQH',
'EQH RFP Team':'EQH',
'Equiatble Advisors':'EQH',
'Equitabe':'EQH',
'Equitable':'EQH',
'Equitable Adviosr':'EQH',
'Equitable Adviosrs':'EQH',
'Equitable Advisor':'EQH',
'Equitable Advisor, LLC':'EQH',
'Equitable Advisors':'EQH',
'Equitable Advisors LLC':'EQH',
'Equitable Advisors, LLC':'EQH',
'Equitable Advsiors':'EQH',
'Equitable Distributors':'EQH',
'Equitable Employee':'EQH',
'Equitable Financial':'EQH',
'Equitable- Texas':'EQH',
'Equitable.':'EQH',
'Equitable.com':'EQH',
'Equitablec':'EQH',
'Equitablw':'EQH',
'Equtable':'EQH',
'Euitable':'EQH',
'Equity Service':'Equity Services',
'Equity Services Inc':'Equity Services',
'Equity Services Inc.':'Equity Services',
'Farmers Agent':'Farmers',
'Farmers Financial':'Farmers',
'Farmers Financial Solution LLC':'Farmers',
'Farmers Insurance':'Farmers',
'Farmers Insurance Group / FFS':'Farmers',
'Farmers Mutual':'Farmers',
'Farmers Financial':'Farmers Financial Solution LLC',
'Farmers Financial Soltuions':'Farmers Financial Solution LLC',
'Farmers Financial Solutions':'Farmers Financial Solution LLC',
'Farmers Insurance':'Farmers Insurance Group',
'First Citizens':'First Citizens Investor Services',
'First Citizens Investor Servic':'First Citizens Investor Services',
'First commamd':'First Command',
'First Command Financial':'First Command',
'First Command Financial Pl Inc':'First Command',
'First heartland':'First Heartland Capital',
'First Heartland Capital, Inc.':'First Heartland Capital',
'First horizion':'First Horizon',
'FirstHorizon':'First Horizon',
'First natl bank':'First National Bank',
'Folger Nolan':'Folger Nolan Douglas',
'Fortune':'Fortune Financial',
'Founder':'Founders Financial',
'Founders':'Founders Financial',
'Franklin Templteon':'Franklin Templeton',
'Frost':'Frost Bank',
'FTB Advisors':'FTB',
'Gallager':'Gallagher & Assoc.',
'Gallagher':'Gallagher & Assoc.',
'Gallegher':'Gallagher & Assoc.',
'Goldman':'Goldman Sachs',
'Grove Point Financial':'Grove Point',
'Grove Point Investments':'Grove Point',
'Grove Point Investments LLC':'Grove Point',
'Grovepoint':'Grove Point',
'Grove Point Investments, LLC':'Grove Point Investments',
'H. Beck':'H Beck',
'Hbeck':'H Beck',
'Haliday':'Halliday',
'Hancock Whitney - MS':'Hancock Whitney',
'Hancock Whtiney - MS':'Hancock Whitney-MS',
'Harbor':'Harbour',
'Harbour Investments':'Harbour',
'Harbour Investments,Inc.':'Harbour',
'Hdvest':'HD Vest',
'Hightower Securities':'Hightower',
'Hornor Townsend & Kent,Inc':'Hornor Townsend & Kent',
'hornor townsend and kent':'Hornor Townsend & Kent',
'Hornor, Townsend, & Kent':'Hornor Townsend & Kent',
'Huntington Bank':'Huntington',
'Huntington Investment':'Huntington',
'huntinton':'Huntington',
'Independence Capital':'Independence Capital Company',
'Independence Capital Co.':'Independence Capital Company',
'Ind Financial Group':'Independent Financial Group',
'Independence Capital Co.':'Independent Financial Group',
'Independence Capital Company':'Independent Financial Group',
'Independent Financial Grp LLC':'Independent Financial Group',
'Independent Financial Grp, LLC':'Independent Financial Group',
'Infinex - Alerus':'Infinex',
'Infinex Investments Inc':'Infinex',
'International Assets':'International asset advisory',
'International Assets Advisory':'International asset advisory',
'Intl asset advisory':'International asset advisory',
'JP Morgan Securities':'J.P. Morgan Securities, LLC',
'J.W.Cole Financial, Inc.':'J.W. Cole Financial',
'James T. Borello':'James T. Borello and Assoc.',
'J P Morgan':'JP Morgan',
'J P Morgan Chase':'JP Morgan',
'J.P Morgan':'JP Morgan',
'J.P. Morgan':'JP Morgan',
'J.P. Morgan Securities, LLC':'JP Morgan',
'J.P.Morgan':'JP Morgan',
'Jo morgan':'JP Morgan',
'JP Morga':'JP Morgan',
'Jp morgan asset mngmt':'JP Morgan',
'JP Morgan Chase':'JP Morgan',
'jpm':'JP Morgan',
'JPM Chase':'JP Morgan',
'JPMC':'JP Morgan',
'JPMorgan':'JP Morgan',
'J.P. Morgan Chase':'JP Morgan Chase',
'JPM Chase':'JP Morgan Chase',
'jpj investments':'JPJ Invesments',
'J.W Cole':'JW Cole',
'J.W. Cole':'JW Cole',
'J.W. Cole Financial Inc.':'JW Cole',
'J.W. Cole Securities':'JW Cole',
'J.W.Cole':'JW Cole',
'JW Cole Securities':'JW Cole',
'jwcole':'JW Cole',
'Kestra Investment Services':'Kestra',
'Kestra Investment':'Kestra Investment Services LLC.',
'Kestra Investment Services':'Kestra Investment Services LLC.',
'Kestra Investment Services LLC':'Kestra Investment Services LLC.',
'Kestra investments':'Kestra Investment Services LLC.',
'Key':'Key Bank',
'Key Investment':'Key Bank',
'Key Investment Services LLC':'Key Bank',
'Key Investments':'Key Bank',
'KeyBank':'Key Bank',
'Key Investment services':'Key Investment Services LLC',
'Key Investment':'Key Investments',
'Kovac':'Kovack',
'Kovack Investments':'Kovack',
'Kovack sec':'Kovack',
'Kovack Securities':'Kovack',
'Kovack Securities Inc.':'Kovack',
'Kovack securities plus 5 reps':'Kovack',
'The leaders group':'Leaders Group',
'Legacy Financial':'Legacy Financial Group',
'Level Four Financial LLC':'Level 4',
'Lincoln Financial':'Lincoln',
'Lincoln Financial - AL':'Lincoln',
'Lincoln Financial Advisor':'Lincoln',
'Lincoln Financial Advisors':'Lincoln',
'Lincoln Financial Sec Corp.':'Lincoln',
'Lincoln Financial Securities':'Lincoln',
'Lincoln Financial Services':'Lincoln',
'Lincoln Investment':'Lincoln',
'Lincoln Investment Group':'Lincoln',
'Lincoln Investment Planning':'Lincoln',
'Lincoln Investment Planning LLC':'Lincoln',
'Lincoln Investment Planning, LLC':'Lincoln',
'Lincoln Investments':'Lincoln',
'Lincoln/Osaic':'Lincoln',
'Lincoln-MS':'Lincoln',
'Lincoln - AL':'Lincoln- AL',
'Lincoln Financial':'Lincoln Financial Advisors',
'Lincoln Financial Advisors Cor':'Lincoln Financial Advisors',
'Lincoln Investment Planning LL':'Lincoln Investment Planning',
'Lincoln Investment':'Lincoln Investments',
'Loomis Sayles':'Loomis',
'LPL - AL':'LPL',
'LPL - MS':'LPL',
'Lpl and M&T':'LPL',
'LPL Fin Serv':'LPL',
'LPL Financial':'LPL',
'LPL Financial Corporation':'LPL',
'LPL Financial LLC':'LPL',
'LPL.':'LPL',
'LPL/ bank':'LPL',
'Lpl-AL':'LPL',
'Lpl-MS':'LPL',
'LPL-STratos':'LPL',
'LPL- AL':'LPL - AL',
'LPL-MS':'LPL - MS',
'Lpl- webster bank':'Lpl webster bank',
'Lpl/webster':'Lpl webster bank',
'Lpl/webster bank':'Lpl webster bank',
'Webster/lpl':'Lpl webster bank',
'LPL schools first':'LPL-schools first',
'M.S. Howells and Co.':'M.S. Howells & Co',
'Madison Avenue':'Madison Ave',
'MeriWest CU':'MeriWest',
'merrill':'Merril Lynch',
'Merrill Lynch':'Merril Lynch',
'Met':'Met Life',
'MetLife':'Met Life',
'mass mutal':'MML',
'Mass Mutual':'MML',
'MassMutual':'MML',
'MML Investors':'MML',
'MML Investors Services':'MML',
'MML Investors Services, LLC':'MML',
'MML Securities':'MML',
'mmli':'MML',
'MML Investors Services':'MML Investor Services, LLC',
'MML Investors Services, LLC':'MML Investor Services, LLC',
'Morgan':'Morgan Stanley',
'Morgan Stanely':'Morgan Stanley',
'Morgan Stanley Smith Barmey':'Morgan Stanley',
'MorganStanley':'Morgan Stanley',
'MS':'Morgan Stanley',
'MS - AL':'Morgan Stanley',
'MS Howell':'MS Howell, or Morgan Stanley?',
'mutual of of omaha':'Mutual of Omaha',
'Mutual Of Omaha Inv Ser. Inc':'Mutual of Omaha',
'nationwide':'National Securities Corp',
'New Bridge Securities':'Newbridge Securities',
'Newbridge':'Newbridge Securities',
'NEXT':'Next Financial',
'Next Financial Group':'Next Financial',
'Next Financial.':'Next Financial',
'NM ASBO':'NMASBO',
'North Oaks Financial':'North Oaks Financial Services',
'OMNI/TSACG':'Omni',
'O&N Equity':'ON Equity',
'O.N Equity':'ON Equity',
'O.N. Equity':'ON Equity',
'O.N.Equity':'ON Equity',
'On equities':'ON Equity',
'ON Investments':'ON Equity',
'The O.N. Equity':'ON Equity',
'One America Securities':'One America',
'One America.':'One America',
'OneAmerica':'One America',
'OneAmerica Securities Inc':'One America',
'Oppeneheimer':'Oppenheimer',
'Osaic Financial':'Osaic',
'Osaic Financial Inc.':'Osaic',
'Osaic Wealth':'Osaic',
'Osaic Wealth, Inc':'Osaic',
'Osaic-Holland and Holland':'Osaic',
'Osaic-MS':'Osaic',
'Osaic-Royal Alliance':'Osaic',
'Osaic-Woodbury':'Osaic',
'Osiac':'Osaic',
'Osaic Institutions Inc':'Osaic Institutions',
'Osaic Institutions, Inc.':'Osaic Institutions',
'Osaic Wealth Inc.':'Osaic Wealth',
'Osaic Wealth, Inc.':'Osaic Wealth',
'Pac Life':'Paclife',
'Palmer 11':'Palmer11',
'Park Ave':'Park Ave Securities',
'Park Avenue':'Park Ave Securities',
'Park Avenue Securities':'Park Ave Securities',
'Park Ave':'Park Avenue',
'Parkland':'Parkland Securities',
'Patelco cu':'Patelco',
'PFS Investments, Inc':'PFS Investments',
'PFS Investments, LLC':'PFS Investments',
'Plan Member':'PlanMember',
'Plan Member Services':'PlanMember',
'Plan members':'PlanMember',
'Planmamber':'PlanMember',
'Planmember securities':'PlanMember',
'Planmember services':'PlanMember',
'PlanMembers':'PlanMember',
'PlanMemeber':'PlanMember',
'PlanMemember':'PlanMember',
'PlanMember Securities Corp.':'PlanMember Securities',
'PNC Bank':'PNC',
'PNC Investments':'PNC',
'PNC Investments LLC':'PNC',
'PNCI':'PNC',
'PNC Investments':'PNC Investments, LLC',
'popular sec':'POPULAR',
'Christine.bronson@primerica.com':'Primerica',
'PFS':'Primerica',
'PFS Investments':'Primerica',
'PFS Investments, Inc':'Primerica',
'Primeirca':'Primerica',
'Primeirica':'Primerica',
'Primeric':'Primerica',
'Primerica Financial Services':'Primerica',
'Principal - AL':'Principal',
'Principal Advisors':'Principal',
'Principal Securities':'Principal',
'Principal Securities, Inc':'Principal',
'Principal Securities, Inc.':'Principal',
'Principal-AL':'Principal',
'Principle':'Principal',
'Principal Advisors.':'Principal Advisors',
'Principal Securities Corporation':'Principal Securities',
'Principal Securities Inc.':'Principal Securities',
'Principal Securities, Inc.':'Principal Securities',
'Private Client Services':'Private client',
'Pru':'Prudential',
'Pruco':'Prudential',
'Pruco Securities':'Prudential',
'Pruco Securities, LLC':'Prudential',
'Prucol':'Prudential',
'prudentail':'Prudential',
'Prudential-MS':'Prudential',
'Pruco Securities':'Prudential Securities',
'Pruco Securities, LLC':'Prudential Securities',
'Purse Kaplan':'Purshe Kaplan Sterling',
'Purshe Kaplan':'Purshe Kaplan Sterling',
'Purshue Kaplan':'Purshe Kaplan Sterling',
'Ray James':'Raymond James',
'Ray Jay':'Raymond James',
'RayJay':'Raymond James',
'Raymond James & Associates':'Raymond James & Associates Inc',
'Ray J':'Raymond James Financial',
'Ray Ja':'Raymond James Financial',
'Ray Jay':'Raymond James Financial',
'Ray Jay.':'Raymond James Financial',
'RayJ':'Raymond James Financial',
'RayJay':'Raymond James Financial',
'Raymond James':'Raymond James Financial',
'Raymond james - AL':'Raymond James Financial',
'Raymond James - MS':'Raymond James Financial',
'Raymond James & Associates':'Raymond James Financial',
'Raymond James Financial Services':'Raymond James Financial',
'Raymond James Financial Svcs':'Raymond James Financial',
'Raymond James Financial Svcs.':'Raymond James Financial',
'RJ':'Raymond James Financial',
'RJ - AL':'Raymond James Financial',
'RJ - MS':'Raymond James Financial',
'RJA':'Raymond James Financial',
'RJFS':'Raymond James Financial',
'RBC Capital':'RBC',
'RBC Wealth Managament':'RBC',
'RBC Wealth Management':'RBC',
'Regions - AL':'Regions bank',
'Rockefeller':'Rockefeller Company',
'Royal Alliancd':'Royal Alliance',
'Royal':'Royal Alliance Associates Inc.',
'Royal Alliance':'Royal Alliance Associates Inc.',
'Royal Alliance Associates Inc':'Royal Alliance Associates Inc.',
'Sage Point':'Sagepoint',
'Sage Point-AL':'Sagepoint - AL',
'Sagepoint-AL':'Sagepoint - AL',
'Santander Securities':'Santander',
'Scipione Wealth':'Scipione Wealth Advisors',
'Scipione Wealth Advisors, Inc':'Scipione Wealth Advisors',
'SEcurian':'Securian Financial',
'Dave@sfmadvisorgroup.com':'SFM Advisor Group',
'Signature Financial':'Signature Financial Group',
'Signature Financial grp':'Signature Financial Group',
'Silver oak':'Silver Oaks',
'Silveroak':'Silver Oaks',
'SJI Financial- LPL':'SJI Financial - LPL',
'Soaring Eagles Community School':'Soaring Eagle Community School',
'Stern Agee':'Sterne Agee',
'sterne':'Sterne Agee',
'Stone Crest':'Stonecrest',
'Stonecrest partners':'Stonecrest',
'The Strategic Fin Alliance':'Strategic Financial Alliance',
'Symmetra':'Symmerty',
'Symmetry':'Symmerty',
'Synovus - AL':'Synovus',
't rowe':'T Rowe Price',
'T. Rowe Price':'T Rowe Price',
'The Investment Center Inc':'The Investment Center',
'The Investment Center, Inc':'The Investment Center',
'Triad Advisors':'Triad',
'Truist Investment Services':'Truist',
'Union':'UnionBanc',
'Union bank':'UnionBanc',
'United Planning Corp':'United Planners',
'U.S. bank':'US Bank',
'USBank':'US Bank',
'Vanderbilt securities':'Vanderbilt',
'Well':'Wells Fargo',
'Well Fargo':'Wells Fargo',
'Wellls':'Wells Fargo',
'Wells':'Wells Fargo',
'Wells Fargo - AL':'Wells Fargo',
'Wells Fargo Adv. Fin Net':'Wells Fargo',
'Wells Fargo Advisors':'Wells Fargo',
'Wells Fargo- AL':'Wells Fargo',
'Wells Fargo Finet':'Wells Fargo',
'Wells Fargo.':'Wells Fargo',
'WellsFargo':'Wells Fargo',
'Welsl Fargo':'Wells Fargo',
'Wellsfargo-AL':'Wells Fargo-AL',
'WF - AL':'WF',
'WFA':'WF',
'Woodbury':'Woodbury Financial',
'Woodbury Financial Serv Inc':'Woodbury Financial',
'Woodbury Financial Serv Inc.':'Woodbury Financial',
'Woodbury Financial Services':'Woodbury Financial',
'Woodbury Financial Services.':'Woodbury Financial'
}

# Cache for standardized names and companies
name_standardization_cache = {}
company_standardization_cache = {}

class EnhancedNameCorrectionSystem:
    """
    An enhanced system for matching and correcting wholesaler agent names against a standard distribution list.
    Incorporates company name mapping and improved name swap detection.
    """
    
    def __init__(self, config=None):
        """
        Initialize the EnhancedNameCorrectionSystem with configuration.
        
        Parameters:
        -----------
        config : dict, default=None
            Configuration dictionary with the following keys:
            - cache_dir: Directory to store cache files
            - batch_size: Size of batches for processing
            - threshold: Threshold for high confidence matches
            - medium_threshold: Threshold for medium confidence matches
        """
        # Default configuration
        self.config = {
            'cache_dir': './name_correction_cache',
            'batch_size': 5000,
            'threshold': 0.95,  # Increased threshold for high confidence
            'medium_threshold': 0.80  # Threshold for medium confidence
        }
        
        # Update with provided configuration
        if config:
            self.config.update(config)
        
        # Create cache directory if it doesn't exist
        if not os.path.exists(self.config['cache_dir']):
            os.makedirs(self.config['cache_dir'])
        
        # Initialize matcher
        self.matcher = None
    
    def preprocess_distribution_list(self, dist_list_path, first_name_col, last_name_col, company_col=None):
        """
        Preprocess the distribution list and create indices.
        
        Parameters:
        -----------
        dist_list_path : str
            Path to the distribution list Excel file
        first_name_col : str
            Column name for first names in the distribution list
        last_name_col : str
            Column name for last names in the distribution list
        company_col : str, default=None
            Column name for company names in the distribution list
        
        Returns:
        --------
        bool
            True if preprocessing was successful
        """
        logger.info(f"Preprocessing distribution list: {dist_list_path}")
        
        try:
            # Load distribution list
            dist_list_df = pd.read_excel(dist_list_path)
            
            # Validate required columns
            required_cols = [first_name_col, last_name_col]
            for col in required_cols:
                if col not in dist_list_df.columns:
                    logger.error(f"Required column '{col}' not found in distribution list")
                    return False
            
            # Check if company column exists
            has_company = company_col is not None and company_col in dist_list_df.columns
            
            # Rename columns to standard names for internal processing
            column_mapping = {
                first_name_col: 'first_name',
                last_name_col: 'last_name'
            }
            
            if has_company:
                column_mapping[company_col] = 'company'
            
            dist_list_df = dist_list_df.rename(columns=column_mapping)
            
            # Add empty company column if not provided
            if not has_company:
                dist_list_df['company'] = ""
            
            # Initialize matcher
            self.matcher = EnhancedNameMatcher(
                dist_list_df,
                cache_dir=self.config['cache_dir']
            )
            
            logger.info("Distribution list preprocessing completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error preprocessing distribution list: {str(e)}")
            return False
    
    def correct_names(self, input_path, output_path, first_name_col, last_name_col, company_col=None):
        """
        Correct names in the input file and save results to the output file.
        
        Parameters:
        -----------
        input_path : str
            Path to the input Excel file with names to correct
        output_path : str
            Path to save the output Excel file with corrected names
        first_name_col : str
            Column name for first names in the input file
        last_name_col : str
            Column name for last names in the input file
        company_col : str, default=None
            Column name for company names in the input file
        
        Returns:
        --------
        bool
            True if correction was successful
        """
        logger.info(f"Correcting names from: {input_path}")
        
        try:
            # Check if matcher is initialized
            if not self.matcher:
                logger.error("Matcher not initialized. Run preprocess_distribution_list first.")
                return False
            
            # Load input data
            input_df = pd.read_excel(input_path)
            
            # Validate required columns
            required_cols = [first_name_col, last_name_col]
            for col in required_cols:
                if col not in input_df.columns:
                    logger.error(f"Required column '{col}' not found in input file")
                    return False
            
            # Check if company column exists
            has_company = company_col is not None and company_col in input_df.columns
            
            # Rename columns to standard names for internal processing
            column_mapping = {
                first_name_col: 'first_name',
                last_name_col: 'last_name'
            }
            
            if has_company:
                column_mapping[company_col] = 'company'
            
            input_df = input_df.rename(columns=column_mapping)
            
            # Add empty company column if not provided
            if not has_company:
                input_df['company'] = ""
            
            # Correct names
            start_time = time.time()
            result_df = self.matcher.correct_names_df(
                input_df,
                threshold=self.config['threshold'],
                medium_threshold=self.config['medium_threshold'],
                batch_size=self.config['batch_size']
            )
            processing_time = time.time() - start_time
            
            # Rename columns back to original names
            result_df = result_df.rename(columns={
                'first_name': first_name_col,
                'last_name': last_name_col
            })
            
            if has_company:
                result_df = result_df.rename(columns={
                    'company': company_col,
                    'corrected_company': f"corrected_{company_col}"
                })
            else:
                # Remove company-related columns if not needed
                result_df = result_df.drop(columns=['company', 'corrected_company'], errors='ignore')
            
            # Generate summary statistics
            total_records = len(result_df)
            confidence_counts = result_df['match_confidence'].value_counts()
            high_confidence = confidence_counts.get('high', 0)
            medium_confidence = confidence_counts.get('medium', 0)
            no_match = confidence_counts.get('no_match', 0)
            
            # Log summary
            logger.info(f"Processing completed in {processing_time:.2f} seconds")
            logger.info(f"Total records processed: {total_records}")
            logger.info(f"High confidence matches: {high_confidence} ({high_confidence/total_records:.1%})")
            logger.info(f"Medium confidence matches: {medium_confidence} ({medium_confidence/total_records:.1%})")
            logger.info(f"No matches: {no_match} ({no_match/total_records:.1%})")
            
            # Save results
            result_df.to_excel(output_path, index=False)
            logger.info(f"Results saved to: {output_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error correcting names: {str(e)}")
            return False

class EnhancedNameMatcher:
    """
    An enhanced class to match and correct wholesaler agent names against a standard distribution list.
    Incorporates company name mapping and improved name swap detection.
    """
    
    def __init__(self, dist_list_df, cache_dir=None):
        """
        Initialize the EnhancedNameMatcher with a standard distribution list.
        
        Parameters:
        -----------
        dist_list_df : pandas.DataFrame
            DataFrame containing the standard distribution list with columns 'first_name', 'last_name', and 'company'
        cache_dir : str, default=None
            Directory to store cache files. If None, no caching is used.
        """
        self.dist_list_df = dist_list_df
        self.cache_dir = cache_dir
        
        # Create cache directory if specified
        if self.cache_dir and not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
        
        # Create exact match lookup dictionaries
        self._create_exact_match_lookups()
        
        # Load or create indices
        self._load_or_create_indices()
    
    def _standardize_name(self, name):
        """
        Standardize a name by converting to lowercase, removing punctuation,
        and handling common nicknames.
        
        Parameters:
        -----------
        name : str
            The name to standardize
            
        Returns:
        --------
        str
            Standardized name
        """
        # Check cache first
        if name in name_standardization_cache:
            return name_standardization_cache[name]
        
        if not isinstance(name, str) or pd.isna(name) or name == "" or str(name).strip() == "":
            result = ""
        else:
            # Convert to lowercase
            result = str(name).lower()
            
            # Remove punctuation and extra spaces
            result = re.sub(r'[^\w\s]', '', result)
            result = re.sub(r'\s+', ' ', result).strip()
            
            # Handle common nicknames
            if result in NICKNAME_MAP:
                result = NICKNAME_MAP[result]
        
        # Cache the result
        name_standardization_cache[name] = result
        return result
    
    def _standardize_company(self, company):
        """
        Standardize a company name by converting to lowercase, removing punctuation,
        applying company mapping, and removing common words like "Inc", "LLC", etc.
        
        Parameters:
        -----------
        company : str
            The company name to standardize
            
        Returns:
        --------
        str
            Standardized company name
        """
        # Check cache first
        if company in company_standardization_cache:
            return company_standardization_cache[company]
        
        if not isinstance(company, str) or pd.isna(company) or company == "" or str(company).strip() == "":
            result = ""
        else:
            # Convert to lowercase
            result = str(company).lower()
            
            # Remove punctuation and extra spaces
            result = re.sub(r'[^\w\s]', '', result)
            result = re.sub(r'\s+', ' ', result).strip()
            
            # Apply company mapping
            if result in COMPANY_MAP:
                result = COMPANY_MAP[result]
            
            # Remove common company suffixes
            common_suffixes = [
                ' inc', ' llc', ' ltd', ' limited', ' corp', ' corporation',
                ' company', ' co', ' group', ' holdings', ' advisors', ' advisers',
                ' advisory', ' financial', ' services', ' investments', ' securities',
                ' management', ' asset', ' capital', ' partners', ' associates',
                ' consulting', ' solutions', ' international', ' national', ' global'
            ]
            
            for suffix in common_suffixes:
                if result.endswith(suffix):
                    result = result[:-len(suffix)]
            
            # Remove common abbreviations
            result = re.sub(r'\b(inc|llc|ltd|co|corp)\b', '', result)
            
            # Clean up extra spaces
            result = re.sub(r'\s+', ' ', result).strip()
        
        # Cache the result
        company_standardization_cache[company] = result
        return result
    
    def _create_exact_match_lookups(self):
        """Create lookup dictionaries for exact matches."""
        logger.info("Creating exact match lookups...")
        
        # Create lookup for exact first name + last name matches
        self.exact_name_lookup = {}
        for idx, row in self.dist_list_df.iterrows():
            first_name = row['first_name'] if isinstance(row['first_name'], str) else ""
            last_name = row['last_name'] if isinstance(row['last_name'], str) else ""
            
            if first_name and last_name:
                key = (str(first_name).lower(), str(last_name).lower())
                self.exact_name_lookup[key] = idx
        
        # Create lookup for exact last name + company matches
        self.last_name_company_lookup = {}
        for idx, row in self.dist_list_df.iterrows():
            last_name = row['last_name'] if isinstance(row['last_name'], str) else ""
            company = row['company'] if isinstance(row['company'], str) else ""
            
            if last_name and company:
                std_company = self._standardize_company(company)
                if std_company:
                    key = (str(last_name).lower(), std_company)
                    if key not in self.last_name_company_lookup:
                        self.last_name_company_lookup[key] = []
                    self.last_name_company_lookup[key].append(idx)
        
        # Create lookup for exact first name + company matches
        self.first_name_company_lookup = {}
        for idx, row in self.dist_list_df.iterrows():
            first_name = row['first_name'] if isinstance(row['first_name'], str) else ""
            company = row['company'] if isinstance(row['company'], str) else ""
            
            if first_name and company:
                std_company = self._standardize_company(company)
                if std_company:
                    key = (str(first_name).lower(), std_company)
                    if key not in self.first_name_company_lookup:
                        self.first_name_company_lookup[key] = []
                    self.first_name_company_lookup[key].append(idx)
        
        # Create lookup for swapped name matches (first name as last name and vice versa)
        self.swapped_name_lookup = {}
        for idx, row in self.dist_list_df.iterrows():
            first_name = row['first_name'] if isinstance(row['first_name'], str) else ""
            last_name = row['last_name'] if isinstance(row['last_name'], str) else ""
            
            if first_name and last_name:
                key = (str(last_name).lower(), str(first_name).lower())  # Swapped order
                self.swapped_name_lookup[key] = idx
    def _load_or_create_indices(self):
        """Load indices from cache or create them if not available."""
        cache_file = None
        if self.cache_dir:
            cache_file = os.path.join(self.cache_dir, 'enhanced_name_matcher_indices.pkl')
        
        if cache_file and os.path.exists(cache_file):
            # Load indices from cache
            logger.info(f"Loading indices from cache: {cache_file}")
            with open(cache_file, 'rb') as f:
                cache_data = pickle.load(f)
                self.dist_list_df = cache_data['dist_list_df']
                self.last_initial_index = cache_data['last_initial_index']
                self.first_initial_index = cache_data.get('first_initial_index', defaultdict(list))
                self.last_soundex_index = cache_data['last_soundex_index']
                self.first_soundex_index = cache_data['first_soundex_index']
                self.first_two_chars_index = cache_data['first_two_chars_index']
                self.last_two_chars_index = cache_data['last_two_chars_index']
                self.company_word_index = cache_data['company_word_index']
        else:
            # Create indices from scratch
            logger.info("Creating indices...")
            start_time = time.time()
            
            # Preprocess the distribution list
            self._preprocess_dist_list()
            
            # Create blocking indices
            self._create_blocking_indices()
            
            # Save indices to cache if cache_dir is specified
            if cache_file:
                logger.info(f"Saving indices to cache: {cache_file}")
                with open(cache_file, 'wb') as f:
                    pickle.dump({
                        'dist_list_df': self.dist_list_df,
                        'last_initial_index': self.last_initial_index,
                        'first_initial_index': self.first_initial_index,
                        'last_soundex_index': self.last_soundex_index,
                        'first_soundex_index': self.first_soundex_index,
                        'first_two_chars_index': self.first_two_chars_index,
                        'last_two_chars_index': self.last_two_chars_index,
                        'company_word_index': self.company_word_index
                    }, f)
            
            logger.info(f"Indices created in {time.time() - start_time:.2f} seconds")
    
    def _preprocess_dist_list(self):
        """Preprocess the distribution list for efficient matching."""
        logger.info("Preprocessing distribution list...")
        
        # Create standardized versions of names
        self.dist_list_df['first_name_std'] = self.dist_list_df['first_name'].apply(self._standardize_name)
        self.dist_list_df['last_name_std'] = self.dist_list_df['last_name'].apply(self._standardize_name)
        self.dist_list_df['company_std'] = self.dist_list_df['company'].apply(self._standardize_company)
        
        # Create phonetic keys
        self.dist_list_df['first_name_soundex'] = self.dist_list_df['first_name_std'].apply(
            lambda x: jellyfish.soundex(x) if x else ""
        )
        self.dist_list_df['last_name_soundex'] = self.dist_list_df['last_name_std'].apply(
            lambda x: jellyfish.soundex(x) if x else ""
        )
        
        # Create first letter indices
        self.dist_list_df['first_name_initial'] = self.dist_list_df['first_name_std'].str[0:1]
        self.dist_list_df['last_name_initial'] = self.dist_list_df['last_name_std'].str[0:1]
        
        # Create first two characters indices (for more precise blocking)
        self.dist_list_df['first_name_two_chars'] = self.dist_list_df['first_name_std'].str[0:2]
        self.dist_list_df['last_name_two_chars'] = self.dist_list_df['last_name_std'].str[0:2]
        
        # Create company words for indexing
        self.dist_list_df['company_words'] = self.dist_list_df['company_std'].apply(
            lambda x: set(x.split()) if x else set()
        )
    
    def _create_blocking_indices(self):
        """Create indices for blocking to reduce comparison space."""
        logger.info("Creating blocking indices...")
        
        # Last name initial index
        self.last_initial_index = defaultdict(list)
        for idx, row in self.dist_list_df.iterrows():
            if row['last_name_initial']:
                self.last_initial_index[row['last_name_initial']].append(idx)
                
        # First name initial index
        self.first_initial_index = defaultdict(list)
        for idx, row in self.dist_list_df.iterrows():
            if row['first_name_initial']:
                self.first_initial_index[row['first_name_initial']].append(idx)
        
        # Soundex indices
        self.last_soundex_index = defaultdict(list)
        for idx, row in self.dist_list_df.iterrows():
            if row['last_name_soundex']:
                self.last_soundex_index[row['last_name_soundex']].append(idx)
        
        self.first_soundex_index = defaultdict(list)
        for idx, row in self.dist_list_df.iterrows():
            if row['first_name_soundex']:
                self.first_soundex_index[row['first_name_soundex']].append(idx)
        
        # First two characters indices (for more precise blocking)
        self.first_two_chars_index = defaultdict(list)
        for idx, row in self.dist_list_df.iterrows():
            if len(row['first_name_std']) >= 2:
                self.first_two_chars_index[row['first_name_two_chars']].append(idx)
        
        self.last_two_chars_index = defaultdict(list)
        for idx, row in self.dist_list_df.iterrows():
            if len(row['last_name_std']) >= 2:
                self.last_two_chars_index[row['last_name_two_chars']].append(idx)
        
        # Company word index
        self.company_word_index = defaultdict(list)
        for idx, row in self.dist_list_df.iterrows():
            for word in row['company_words']:
                if len(word) >= 2:  # Only index words with at least 2 characters
                    self.company_word_index[word].append(idx)
    
    def _expand_initial(self, initial):
        """
        Get potential full names for an initial.
        
        Parameters:
        -----------
        initial : str
            The initial letter
            
        Returns:
        --------
        list
            List of indices of names starting with this initial
        """
        if not initial or len(initial) == 0:
            return []
        
        initial = str(initial)[0].lower()
        return self.dist_list_df[self.dist_list_df['first_name_initial'] == initial].index.tolist()
    def _check_exact_match(self, first_name, last_name, company):
        """
        Check for exact matches in the distribution list.
        
        Parameters:
        -----------
        first_name : str
            First name to match
        last_name : str
            Last name to match
        company : str
            Company name to match
            
        Returns:
        --------
        tuple
            (match_found, match_idx, match_type)
        """
        # Check for exact first name + last name match
        if first_name and last_name:
            key = (str(first_name).lower(), str(last_name).lower())
            if key in self.exact_name_lookup:
                idx = self.exact_name_lookup[key]
                return True, idx, "exact_name"
        
        # Check for swapped name match (first name as last name and vice versa)
        if first_name and last_name:
            key = (str(first_name).lower(), str(last_name).lower())  # Swapped order
            if key in self.swapped_name_lookup:
                idx = self.swapped_name_lookup[key]
                return True, idx, "swapped_name"
        
        # Check for exact last name + company match
        if last_name and company:
            std_company = self._standardize_company(company)
            if std_company:
                key = (str(last_name).lower(), std_company)
                if key in self.last_name_company_lookup:
                    # If multiple matches, prefer the one with matching first name if available
                    matches = self.last_name_company_lookup[key]
                    if first_name and len(matches) > 1:
                        for idx in matches:
                            if str(self.dist_list_df.loc[idx, 'first_name']).lower() == str(first_name).lower():
                                return True, idx, "exact_last_name_company_first_name"
                    
                    # Otherwise return the first match
                    return True, matches[0], "exact_last_name_company"
        
        # Check for exact first name + company match (for cases with missing last name)
        if first_name and company and not last_name:
            std_company = self._standardize_company(company)
            if std_company:
                key = (str(first_name).lower(), std_company)
                if key in self.first_name_company_lookup:
                    # Return the first match
                    matches = self.first_name_company_lookup[key]
                    return True, matches[0], "exact_first_name_company"
        
        # Check for initial + last name + company match
        if len(str(first_name).strip()) == 1 and last_name and company:
            std_company = self._standardize_company(company)
            if std_company:
                # Get all records with matching last name and company
                key = (str(last_name).lower(), std_company)
                if key in self.last_name_company_lookup:
                    matches = self.last_name_company_lookup[key]
                    # Check if any match has a first name starting with the initial
                    for idx in matches:
                        dist_first_name = str(self.dist_list_df.loc[idx, 'first_name']).lower()
                        if dist_first_name.startswith(str(first_name).lower()):
                            return True, idx, "initial_last_name_company"
                
                # If no match found with company, try just last name + initial
                last_name_matches = []
                for idx, row in self.dist_list_df.iterrows():
                    if str(row['last_name']).lower() == str(last_name).lower():
                        last_name_matches.append(idx)
                
                for idx in last_name_matches:
                    dist_first_name = str(self.dist_list_df.loc[idx, 'first_name']).lower()
                    if dist_first_name.startswith(str(first_name).lower()):
                        return True, idx, "initial_last_name"
        
        return False, None, None
    def _get_candidate_indices(self, first_name, last_name, company):
        """
        Get candidate indices from the distribution list for a given name.
        Uses multiple blocking strategies to reduce comparison space.
        
        Parameters:
        -----------
        first_name : str
            First name to match
        last_name : str
            Last name to match
        company : str
            Company name to match
            
        Returns:
        --------
        set
            Set of candidate indices
        """
        candidates = set()
        
        # Standardize input names
        first_std = self._standardize_name(first_name)
        last_std = self._standardize_name(last_name)
        company_std = self._standardize_company(company)
        
        # Check if first name is an initial
        is_first_initial = len(first_std) == 1
        
        # Check if last name is an initial
        is_last_initial = len(last_std) == 1
        
        # Get phonetic keys
        try:
            first_soundex = jellyfish.soundex(first_std) if first_std and not is_first_initial else ""
            last_soundex = jellyfish.soundex(last_std) if last_std and not is_last_initial else ""
        except:
            first_soundex = ""
            last_soundex = ""
        
        # Get name initials
        first_initial = first_std[0:1] if first_std else ""
        last_initial = last_std[0:1] if last_std else ""
        
        # Get first two characters (for more precise blocking)
        first_two_chars = first_std[0:2] if len(first_std) >= 2 else ""
        last_two_chars = last_std[0:2] if len(last_std) >= 2 else ""
        
        # Get company words
        company_words = set(company_std.split()) if company_std else set()
        
        # Special handling for initial first name
        if is_first_initial and last_std:
            # First try to find exact matches for last name
            last_name_matches = []
            for idx, row in self.dist_list_df.iterrows():
                if str(row['last_name']).lower() == str(last_name).lower():
                    last_name_matches.append(idx)
            
            # Then filter by first initial
            initial_matches = []
            for idx in last_name_matches:
                dist_first_name = str(self.dist_list_df.loc[idx, 'first_name']).lower()
                if dist_first_name.startswith(first_initial):
                    initial_matches.append(idx)
            
            if initial_matches:
                candidates.update(initial_matches)
                # If we found good matches, return early to prioritize these
                if len(candidates) > 0:
                    return candidates
        
        # Multi-level blocking strategy
        
        # 1. Try most restrictive blocking first (both first and last two chars)
        if first_two_chars and last_two_chars:
            first_candidates = set(self.first_two_chars_index.get(first_two_chars, []))
            last_candidates = set(self.last_two_chars_index.get(last_two_chars, []))
            combined = first_candidates.intersection(last_candidates)
            if combined:
                candidates.update(combined)
        
        # 2. If not enough candidates, try soundex blocking
        if len(candidates) < 50 and first_soundex and last_soundex:
            first_candidates = set(self.first_soundex_index.get(first_soundex, []))
            last_candidates = set(self.last_soundex_index.get(last_soundex, []))
            combined = first_candidates.intersection(last_candidates)
            if combined:
                candidates.update(combined)
        
        # 3. If still not enough candidates, try last name initial + first name soundex
        if len(candidates) < 50 and last_initial and first_soundex:
            last_candidates = set(self.last_initial_index.get(last_initial, []))
            first_candidates = set(self.first_soundex_index.get(first_soundex, []))
            combined = last_candidates.intersection(first_candidates)
            if combined:
                candidates.update(combined)
        
        # 4. If still not enough candidates, try first name initial + last name soundex
        if len(candidates) < 50 and first_initial and last_soundex:
            first_candidates = set(self.first_initial_index.get(first_initial, []))
            last_candidates = set(self.last_soundex_index.get(last_soundex, []))
            combined = first_candidates.intersection(last_candidates)
            if combined:
                candidates.update(combined)
        
        # 5. If still not enough candidates, use just last name initial
        if len(candidates) < 50 and last_initial:
            candidates.update(self.last_initial_index.get(last_initial, []))
        
        # 6. If still not enough candidates, use just first name initial
        if len(candidates) < 50 and first_initial:
            candidates.update(self.first_initial_index.get(first_initial, []))
        
        # 7. Handle first name initial
        if is_first_initial:
            initial_candidates = set(self._expand_initial(first_std))
            if last_initial:
                last_candidates = set(self.last_initial_index.get(last_initial, []))
                initial_candidates = initial_candidates.intersection(last_candidates)
            candidates.update(initial_candidates)
        
        # 8. Use company words to find additional candidates
        if len(candidates) < 100 and company_words:
            company_candidates = set()
            for word in company_words:
                if len(word) >= 2:  # Only use words with at least 2 characters
                    company_candidates.update(self.company_word_index.get(word, []))
            
            # If we have both company candidates and name candidates, prioritize their intersection
            if company_candidates and candidates:
                intersection = company_candidates.intersection(candidates)
                if intersection:
                    # Prioritize the intersection but keep some other candidates
                    candidates = intersection.union(set(list(candidates)[:50]))
            elif company_candidates:
                candidates.update(company_candidates)
        
        # 9. If no candidates found, use last name soundex as fallback
        if not candidates and last_soundex:
            candidates.update(self.last_soundex_index.get(last_soundex, []))
        
        # 10. If still no candidates, use first name soundex as fallback
        if not candidates and first_soundex:
            candidates.update(self.first_soundex_index.get(first_soundex, []))
        
        # 11. If still no candidates, return a limited set of random indices as last resort
        if not candidates:
            # Return a random sample of indices to avoid comparing against the entire dataset
            candidates = set(np.random.choice(len(self.dist_list_df), min(500, len(self.dist_list_df)), replace=False))
        
        # Limit the number of candidates to prevent performance issues
        if len(candidates) > 1000:
            candidates = set(list(candidates)[:1000])
        
        return candidates
    def _calculate_similarity_scores(self, first_name, last_name, company, candidate_idx):
        """
        Calculate similarity scores between input name and a candidate.
        
        Parameters:
        -----------
        first_name : str
            First name to match
        last_name : str
            Last name to match
        company : str
            Company name to match
        candidate_idx : int
            Index of candidate in distribution list
            
        Returns:
        --------
        dict
            Dictionary of similarity scores
        """
        candidate = self.dist_list_df.iloc[candidate_idx]
        
        # Standardize input names
        first_std = self._standardize_name(first_name)
        last_std = self._standardize_name(last_name)
        company_std = self._standardize_company(company)
        
        # Get candidate standardized names
        candidate_first_std = candidate['first_name_std']
        candidate_last_std = candidate['last_name_std']
        candidate_company_std = candidate['company_std']
        
        # Calculate various similarity metrics
        scores = {}
        
        # Exact match checks (with higher weights)
        scores['exact_first_match'] = 1.0 if first_std and first_std == candidate_first_std else 0.0
        scores['exact_last_match'] = 1.0 if last_std and last_std == candidate_last_std else 0.0
        scores['exact_company_match'] = 1.0 if company_std and company_std == candidate_company_std else 0.0
        
        # Jaro-Winkler similarity (good for names)
        scores['first_jaro'] = jellyfish.jaro_winkler_similarity(first_std, candidate_first_std) if first_std and candidate_first_std else 0
        scores['last_jaro'] = jellyfish.jaro_winkler_similarity(last_std, candidate_last_std) if last_std and candidate_last_std else 0
        
        # Company similarity
        if company_std and candidate_company_std:
            scores['company_jaro'] = jellyfish.jaro_winkler_similarity(company_std, candidate_company_std)
            
            # Check for company word overlap
            company_words = set(company_std.split())
            candidate_company_words = set(candidate_company_std.split())
            if company_words and candidate_company_words:
                overlap = len(company_words.intersection(candidate_company_words))
                total = len(company_words.union(candidate_company_words))
                scores['company_word_overlap'] = overlap / total if total > 0 else 0
            else:
                scores['company_word_overlap'] = 0
        else:
            scores['company_jaro'] = 0
            scores['company_word_overlap'] = 0
        
        # Levenshtein distance (normalized)
        if first_std and candidate_first_std:
            lev_dist = jellyfish.levenshtein_distance(first_std, candidate_first_std)
            max_len = max(len(first_std), len(candidate_first_std))
            scores['first_lev'] = 1 - (lev_dist / max_len) if max_len > 0 else 0
        else:
            scores['first_lev'] = 0
            
        if last_std and candidate_last_std:
            lev_dist = jellyfish.levenshtein_distance(last_std, candidate_last_std)
            max_len = max(len(last_std), len(candidate_last_std))
            scores['last_lev'] = 1 - (lev_dist / max_len) if max_len > 0 else 0
        else:
            scores['last_lev'] = 0
        
        # N-gram similarity (using rapidfuzz)
        scores['first_ngram'] = fuzz.token_sort_ratio(first_std, candidate_first_std) / 100 if first_std and candidate_first_std else 0
        scores['last_ngram'] = fuzz.token_sort_ratio(last_std, candidate_last_std) / 100 if last_std and candidate_last_std else 0
        
        # Handle first name initial case
        if len(first_std) == 1 and candidate_first_std and candidate_first_std.startswith(first_std):
            scores['first_initial_match'] = 1.0
        else:
            scores['first_initial_match'] = 0.0
        
        # Handle last name initial case
        if len(last_std) == 1 and candidate_last_std and candidate_last_std.startswith(last_std):
            scores['last_initial_match'] = 1.0
        else:
            scores['last_initial_match'] = 0.0
        
        # Check for name order confusion (first/last swapped)
        swapped_first_jaro = jellyfish.jaro_winkler_similarity(first_std, candidate_last_std) if first_std and candidate_last_std else 0
        swapped_last_jaro = jellyfish.jaro_winkler_similarity(last_std, candidate_first_std) if last_std and candidate_first_std else 0
        
        normal_name_score = (scores['first_jaro'] + scores['last_jaro']) / 2 if first_std and last_std else 0
        swapped_name_score = (swapped_first_jaro + swapped_last_jaro) / 2 if first_std and last_std else 0
        
        scores['possible_swap'] = swapped_name_score > normal_name_score
        scores['swapped_score'] = swapped_name_score
        
        # Calculate composite score with weights
        # Last name is weighted more heavily than first name
        # Exact matches get higher weights
        if scores['possible_swap'] and swapped_name_score > 0.8:
            # If names are likely swapped and the swapped score is high, use swapped score
            composite_score = (
                0.3 * swapped_last_jaro +  # Original last name matched against candidate first name
                0.3 * swapped_first_jaro +  # Original first name matched against candidate last name
                0.1 * scores['company_jaro'] +
                0.1 * scores['company_word_overlap'] +
                0.1 * scores['exact_company_match']
            )
        else:
            # Normal scoring
            composite_score = (
                0.25 * scores['last_jaro'] +
                0.15 * scores['last_lev'] +
                0.15 * scores['last_ngram'] +
                0.15 * scores['first_jaro'] +
                0.05 * scores['first_lev'] +
                0.05 * scores['first_ngram'] +
                0.05 * scores['company_jaro'] +
                0.05 * scores['company_word_overlap'] +
                0.2 * scores['exact_last_match'] +  # Bonus for exact last name match
                0.1 * scores['exact_first_match'] +  # Bonus for exact first name match
                0.05 * scores['exact_company_match']  # Bonus for exact company match
            )
        
        # Boost score for initial matches
        if scores['first_initial_match'] > 0:
            composite_score = min(1.0, composite_score + 0.05)
        
        if scores['last_initial_match'] > 0:
            composite_score = min(1.0, composite_score + 0.05)
        
        # Cap the composite score at 1.0
        scores['composite'] = min(1.0, composite_score)
        
        return {
            'candidate_idx': candidate_idx,
            'first_name': candidate['first_name'],
            'last_name': candidate['last_name'],
            'company': candidate['company'],
            'scores': scores
        }
    def match_name(self, first_name, last_name, company, threshold=0.95, medium_threshold=0.80, top_n=3):
        """
        Match a name against the distribution list.
        
        Parameters:
        -----------
        first_name : str
            First name to match
        last_name : str
            Last name to match
        company : str
            Company name to match
        threshold : float, default=0.95
            Threshold for considering a match as high confidence
        medium_threshold : float, default=0.80
            Threshold for considering a match as medium confidence
        top_n : int, default=3
            Number of top candidates to return
            
        Returns:
        --------
        dict
            Dictionary containing match results
        """
        # First check for exact matches
        exact_match_found, exact_match_idx, exact_match_type = self._check_exact_match(first_name, last_name, company)
        
        if exact_match_found:
            # Create a match result with perfect score for exact match
            candidate = self.dist_list_df.iloc[exact_match_idx]
            best_match = {
                'candidate_idx': exact_match_idx,
                'first_name': candidate['first_name'],
                'last_name': candidate['last_name'],
                'company': candidate['company'],
                'scores': {
                    'composite': 1.0,
                    'exact_match': True,
                    'exact_match_type': exact_match_type,
                    'possible_swap': exact_match_type == "swapped_name"
                }
            }
            
            return {
                'input_first_name': first_name,
                'input_last_name': last_name,
                'input_company': company,
                'best_match': best_match,
                'confidence': 'high',
                'top_candidates': [best_match]
            }
        
        # If no exact match, proceed with fuzzy matching
        # Get candidate indices
        candidate_indices = self._get_candidate_indices(first_name, last_name, company)
        
        # Calculate similarity scores for all candidates
        candidates = []
        for idx in candidate_indices:
            candidate = self._calculate_similarity_scores(first_name, last_name, company, idx)
            candidates.append(candidate)
        
        # Sort candidates by composite score
        candidates.sort(key=lambda x: x['scores']['composite'], reverse=True)
        
        # Take top N candidates
        top_candidates = candidates[:top_n]
        
        # Determine confidence level
        if not top_candidates:
            confidence = "no_match"
            best_match = None
        else:
            best_match = top_candidates[0]
            best_score = best_match['scores']['composite']
            
            # Check if the best match is good enough
            if best_score >= threshold:
                confidence = "high"
            elif best_score >= medium_threshold:
                confidence = "medium"
            else:
                # If score is below medium threshold, consider it no match
                confidence = "no_match"
                best_match = None
                top_candidates = []
        
        return {
            'input_first_name': first_name,
            'input_last_name': last_name,
            'input_company': company,
            'best_match': best_match,
            'confidence': confidence,
            'top_candidates': top_candidates
        }
    
    def correct_names_df(self, df, threshold=0.95, medium_threshold=0.80, batch_size=1000):
        """
        Correct names in a DataFrame against the distribution list.
        
        Parameters:
        -----------
        df : pandas.DataFrame
            DataFrame containing names to correct with 'first_name', 'last_name', and 'company' columns
        threshold : float, default=0.95
            Threshold for considering a match as high confidence
        medium_threshold : float, default=0.80
            Threshold for considering a match as medium confidence
        batch_size : int, default=1000
            Size of batches for processing
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with corrected names and match information
        """
        logger.info(f"Processing {len(df)} names...")
        start_time = time.time()
        
        # Create result DataFrame
        result_df = df.copy()
        
        # Add columns for corrected names and match information
        result_df['corrected_first_name'] = None
        result_df['corrected_last_name'] = None
        result_df['corrected_company'] = None
        result_df['match_confidence'] = None
        result_df['composite_score'] = None
        result_df['possible_name_swap'] = None
        
        # Process in batches to avoid memory issues
        total_batches = (len(df) - 1) // batch_size + 1
        
        for i in range(0, len(df), batch_size):
            batch_num = i // batch_size + 1
            logger.info(f"Processing batch {batch_num}/{total_batches}...")
            batch_df = df.iloc[i:i+batch_size]
            
            # Process each name in the batch with progress bar
            for idx, row in tqdm(batch_df.iterrows(), total=len(batch_df), desc=f"Batch {batch_num}/{total_batches}"):
                first_name = row['first_name'] if pd.notna(row['first_name']) else ""
                last_name = row['last_name'] if pd.notna(row['last_name']) else ""
                company = row['company'] if pd.notna(row['company']) else ""
                
                match = self.match_name(first_name, last_name, company, threshold, medium_threshold)
                
                if match['best_match']:
                    result_df.at[idx, 'corrected_first_name'] = match['best_match']['first_name']
                    result_df.at[idx, 'corrected_last_name'] = match['best_match']['last_name']
                    result_df.at[idx, 'corrected_company'] = match['best_match']['company']
                    result_df.at[idx, 'match_confidence'] = match['confidence']
                    result_df.at[idx, 'composite_score'] = match['best_match']['scores']['composite']
                    result_df.at[idx, 'possible_name_swap'] = match['best_match']['scores'].get('possible_swap', False)
                else:
                    # For no match, keep original values
                    result_df.at[idx, 'corrected_first_name'] = first_name
                    result_df.at[idx, 'corrected_last_name'] = last_name
                    result_df.at[idx, 'corrected_company'] = company
                    result_df.at[idx, 'match_confidence'] = 'no_match'
                    result_df.at[idx, 'composite_score'] = 0.0
                    result_df.at[idx, 'possible_name_swap'] = False
        
        processing_time = time.time() - start_time
        logger.info(f"Processing completed in {processing_time:.2f} seconds")
        logger.info(f"Average time per record: {(processing_time / len(df)) * 1000:.2f} ms")
        
        return result_df
def main():
    """Main function with hardcoded parameters."""
    print("\n===== Enhanced Name Correction System =====\n")
    print("This program corrects wholesaler agent names using a standard distribution list.")
    
    # Get distribution list information
    print("=== Distribution List (Reference List) ===")
    dist_list_path = "Agents_Gold_Source_Unique.xlsx"
    dist_first_name_col = "first_name"
    dist_last_name_col = "last_name"
    dist_company_col = "Company"
    
    # Get wholesaler agent list information
    print("\n=== Wholesaler Agent List (Names to Correct) ===")
    input_path = "EDL-Wholesaler Gifts and Entertainment-1-1-25-3-18-25.xlsx"
    input_first_name_col = "Attendee First Name"
    input_last_name_col = "Attendee Last Name"
    input_company_col = "Company"
    
    # Get output file path
    output_path = "EDL-Wholesaler Gifts and Entertainment-1-1-25-3-18-25-model correction-v3.2.xlsx"
    
    batch_size = 5000
    threshold = 0.95
    medium_threshold = 0.80
    cache_dir = "./name_correction_cache"
    
    # Create configuration
    config = {
        'cache_dir': cache_dir,
        'batch_size': batch_size,
        'threshold': threshold,
        'medium_threshold': medium_threshold
    }
    
    # Initialize system
    print("\nInitializing enhanced name correction system...")
    system = EnhancedNameCorrectionSystem(config)
    
    # Preprocess distribution list
    print(f"\nPreprocessing distribution list: {dist_list_path}")
    success = system.preprocess_distribution_list(dist_list_path, dist_first_name_col, dist_last_name_col, dist_company_col)
    if not success:
        print("Error: Failed to preprocess distribution list. Check the log file for details.")
        return 1
    
    # Correct names
    print(f"\nCorrecting names from: {input_path}")
    print(f"This may take some time depending on the size of your files...")
    success = system.correct_names(input_path, output_path, input_first_name_col, input_last_name_col, input_company_col)
    if not success:
        print("Error: Failed to correct names. Check the log file for details.")
        return 1
    
    print(f"\nName correction completed successfully!")
    print(f"Corrected names saved to: {output_path}")
    print("\nCheck the log file for detailed statistics and information.")
    
    return 0


if __name__ == "__main__":
    try:
        exit(main())
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        exit(1)
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        logger.exception("Unhandled exception")
        exit(1) 

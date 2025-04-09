import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz, process
import re
import os
from datetime import datetime

company_names_map = {
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

def clean_name(name):
    """Clean a name by removing special characters, extra spaces, and converting to lowercase."""
    if pd.isna(name):
        return ""
    # Convert to string if not already
    name = str(name)
    # Remove special characters and extra spaces
    name = re.sub(r'[^\w\s]', '', name)
    # Convert to lowercase and strip
    return name.lower().strip()

def find_best_match(name, reference_list, threshold=80):
    """Find the best match for a name in a reference list using fuzzy matching."""
    if not name or name == "":
        return None
    
    # Get the top 3 matches
    matches = process.extract(name, reference_list, limit=3, scorer=fuzz.token_sort_ratio)
    
    # Return the best match if it's above the threshold
    if matches and matches[0][1] >= threshold:
        return matches[0][0]
    return None

def correct_names(input_file, gold_source_file, output_file=None):
    """
    Correct names in the input file based on the gold source file.
    
    Args:
        input_file: Path to the input Excel file with names to correct
        gold_source_file: Path to the gold source Excel file with correct names
        output_file: Path to save the corrected file (if None, will generate one)
    
    Returns:
        Path to the output file
    """
    print(f"Loading input file: {input_file}")
    input_df = pd.read_excel(input_file)
    
    print(f"Loading gold source file: {gold_source_file}")
    gold_df = pd.read_excel(gold_source_file)
    
    # Create clean versions of the gold source names for matching
    gold_df['clean_first_name'] = gold_df['first_name'].apply(clean_name)
    gold_df['clean_last_name'] = gold_df['last_name'].apply(clean_name)
    gold_df['clean_company'] = gold_df['Company'].apply(clean_name)
    
    # Create reference lists for matching
    first_names_ref = gold_df['clean_first_name'].dropna().unique().tolist()
    last_names_ref = gold_df['clean_last_name'].dropna().unique().tolist()
    companies_ref = gold_df['clean_company'].dropna().unique().tolist()
    
    # Create a dictionary for quick lookups
    first_name_dict = dict(zip(gold_df['clean_first_name'], gold_df['first_name']))
    last_name_dict = dict(zip(gold_df['clean_last_name'], gold_df['last_name']))
    company_dict = dict(zip(gold_df['clean_company'], gold_df['Company']))
    
    # Create clean versions of input names
    input_df['clean_first_name'] = input_df['Attendee First Name'].apply(clean_name)
    input_df['clean_last_name'] = input_df['Attendee Last Name'].apply(clean_name)
    input_df['clean_company'] = input_df['Company'].apply(clean_name)
    
    # Create columns for corrected names
    input_df['Corrected First Name'] = input_df['Attendee First Name']
    input_df['Corrected Last Name'] = input_df['Attendee Last Name']
    input_df['Corrected Company'] = input_df['Company']
    
    # Track corrections
    corrections = {
        'first_name': 0,
        'last_name': 0,
        'company': 0,
        'company_map': 0
    }
    
    # First correct company names using the mapping dictionary
    for idx, row in input_df.iterrows():
        # Check if the company name exists in the mapping
        if row['Company'] in company_names_map:
            corrected_company = company_names_map[row['Company']]
            input_df.at[idx, 'Corrected Company'] = corrected_company
            corrections['company_map'] += 1
    
    # Correct names using fuzzy matching
    for idx, row in input_df.iterrows():
        # First name correction
        if row['clean_first_name']:
            best_match = find_best_match(row['clean_first_name'], first_names_ref)
            if best_match and best_match != row['clean_first_name']:
                input_df.at[idx, 'Corrected First Name'] = first_name_dict.get(best_match, row['Attendee First Name'])
                corrections['first_name'] += 1
        
        # Last name correction
        if row['clean_last_name']:
            best_match = find_best_match(row['clean_last_name'], last_names_ref)
            if best_match and best_match != row['clean_last_name']:
                input_df.at[idx, 'Corrected Last Name'] = last_name_dict.get(best_match, row['Attendee Last Name'])
                corrections['last_name'] += 1
        
        # Company correction (only if not already corrected by mapping)
        if row['Company'] not in company_names_map and row['clean_company']:
            best_match = find_best_match(row['clean_company'], companies_ref)
            if best_match and best_match != row['clean_company']:
                input_df.at[idx, 'Corrected Company'] = company_dict.get(best_match, row['Company'])
                corrections['company'] += 1
    
    # Generate output filename if not provided
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(os.path.dirname(input_file), f"Corrected_Wholesaler_Data_{timestamp}.xlsx")
    
    # Save the corrected data
    print(f"Saving corrected data to: {output_file}")
    result_df = input_df.drop(['clean_first_name', 'clean_last_name', 'clean_company'], axis=1)
    result_df.to_excel(output_file, index=False)
    
    # Print summary
    print("\nCorrection Summary:")
    print(f"First Names Corrected: {corrections['first_name']}")
    print(f"Last Names Corrected: {corrections['last_name']}")
    print(f"Companies Corrected via Mapping: {corrections['company_map']}")
    print(f"Companies Corrected via Fuzzy Matching: {corrections['company']}")
    print(f"Total Records Processed: {len(input_df)}")
    
    return output_file

def main():
    # Define file paths
    input_file = r"c:\Users\mbandru2\OneDrive - DXC Production\Equitable\Names-Corrections\EDL-Wholesaler Gifts and Entertainment-1-1-25-3-18-25.xlsx"
    gold_source_file = r"c:\Users\mbandru2\OneDrive - DXC Production\Equitable\Names-Corrections\Agents_Gold_Source_Unique.xlsx"
    
    # Run the correction process
    output_file = correct_names(input_file, gold_source_file)
    print(f"\nProcess completed successfully. Corrected file saved at: {output_file}")

if __name__ == "__main__":
    main()
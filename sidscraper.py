from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from pathlib import Path
from tqdm import tqdm
import requests
import argparse
import pandas
import regex
import time
import csv

def load_arguments():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        '-d', '--directory',
        type = str,
        help = 'Directory to save the output file.'
    )
    ap.add_argument(
        '-n', '--filename',
        type = str,
        help = 'Name of the output file (without extension).'
    )
    return ap.parse_args()

def process_args():
    args = load_arguments()
    ap = argparse.ArgumentParser()
    if args.directory and not Path(args.directory).is_dir():
        ap.error("Directory '" + args.directory + "' does not exist.")
    elif not args.directory:
        args.directory = ''
    if args.filename and any(x in args.filename for x in ['\\', '/']):
        ap.error("Invalid file name. Are you trying to pass a directory as a file name?")
    elif not args.filename:
        args.filename = 'sidscraper_output'
    args.path = args.directory + args.filename
    return args

def std_var_names(string):
    string = regex.sub(r'^\W+|\W+$', '', string)
    string = regex.sub(r'[^\pL]+', '_', string)
    return string.lower()

def clean_str(string, num = False):
    if num:
        string = regex.sub(r'[^\dE.,-]', '', string)
    else:
        string = regex.sub(r'^[^A-Za-z]+|(?!\.)[^\pL]+$', '', string)
        string = regex.sub(r'\s+', ' ', string)
    return string

def set_user_agent():
    fallback = 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2117.157 Safari/537.36'
    user_agent = UserAgent(fallback = fallback, verify_ssl = False)
    return {'User-Agent': user_agent.random}

def get_html(query, parser = 'html.parser', timeout = 10, tries = 2):
    for i in range(tries):
        try:
            header = set_user_agent()
            response = requests.get(query, timeout = timeout, headers = header)
            response.raise_for_status()
        except requests.exceptions.Timeout as err_timeout:
            print(err_timeout)
        except requests.exceptions.RequestException as err:
            raise SystemExit(err)
        else:
            html = BeautifulSoup(response.text, parser)
            break
    return html

def get_family_names():
    print('Extracting family names...', end = '', flush = True)
    data = []
    for i in ['A', 'G']:
        query = 'http://www.theplantlist.org/1.1/browse/' + i + '/'
        try:
            page = get_html(query)
        except UnboundLocalError:
            raise SystemExit('Unable to establish connection with the server.')
        html_families = page.find_all('i', class_ = 'family')
        families = [x.text for x in html_families]
        data.extend(families)
        time.sleep(2)
    return data

def get_styles(html):
    styles = {}
    spans = html.find('p').find_all('span')
    for span in spans:
        try:
            key = std_var_names(span.text)
            value = span['style']
        except (AttributeError, KeyError):
            continue
        styles[key] = value
    return styles

def get_data(html, dict):
    data = []
    for i in html.find_all('p'):
        try:
            taxa = i.find('a').text
        except AttributeError:
            continue
        try:
            mean_seed_weight = i.find('span', {'style': dict['mean_seed_weight']}).text
        except AttributeError:
            mean_seed_weight = ''
        try:
            oil_content = i.find('span', {'style': dict['oil_content']}).text
        except AttributeError:
            oil_content = ''
        try:
            protein_content = i.find('span', {'style': dict['protein_content']}).text
        except AttributeError:
            protein_content = ''
        salt_tolerance = 1 if i.find('span', {'style': dict['salt_tolerance']}) else 0
        values = [clean_str(x, num = True) for x in [mean_seed_weight, oil_content, protein_content]]
        data.append([clean_str(taxa), *values, salt_tolerance])
    return data

def scrape_sid(families):
    res = []
    styles = {}
    got_styles = False
    for family in tqdm(families, desc = 'Extracting seed data'):
        query = 'https://data.kew.org/sid/SidServlet?Clade=&Order=&Family=' + family + '&APG=off&Genus=&Species=&StorBehav=0'
        try:
            page = get_html(query, parser = 'lxml')
        except UnboundLocalError:
            print("Unable to establish connection with the server. Failed request for family:", family)
        else:
            main = page.find('div', id = 'sid')
            n_records = main.find('b').text
            if not got_styles:
                styles.update(get_styles(main))
                got_styles = True
            if n_records[0] == '0':
                continue
            else:
                html_sp = main.find_all('p')[1]
                html_sp_reshaped = str(html_sp).replace('<br/>\n', '</p><p>')
                html_sp = BeautifulSoup(html_sp_reshaped, 'html.parser')
                data = get_data(html_sp, styles)
                res.extend(data)
        finally:
            time.sleep(10)
    return res

def ls_to_csv(data, file_name, cols, sep = ';'):
    df = pandas.DataFrame(data, columns = cols)
    out = df.to_csv(file_name + '.csv', sep = sep, index = False, quoting = csv.QUOTE_ALL)
    return out

def main():
    args = process_args()
    families = get_family_names()
    data = scrape_sid(families)
    ls_to_csv(data, file_name = args.path, cols = ['taxa', 'mean_seed_weight_g', 'perc_oil_content', 'perc_protein_content', 'salt_tolerance'])

if __name__ == '__main__':
    main()

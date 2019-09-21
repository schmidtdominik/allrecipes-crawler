import itertools
import os
import random
from pathlib import Path
from time import time, sleep

import requests
from bs4 import BeautifulSoup, NavigableString
from multiprocessing import Pool
from tqdm import tqdm
import pickle


def tryfloat(n):
    try:
        return float(n)
    except ValueError:
        try:
            return float(n.replace('>', '').replace('<', '').strip())
        except ValueError:
            return n

def get_recipe(id):
    r = requests.get(f'https://www.allrecipes.com/recipe/{id}')
    if r.status_code == 404:
        return None, False, r.status_code, set(), id

    soup = BeautifulSoup(r.text, 'html.parser')

    name = str(soup.title.string.replace('- Allrecipes.com', '').strip())

    if 'Johnsonville' in name:
        return None, False, 'Johnsonville', set(), id

    ingredients = [str(i.string) for i in soup.find_all(class_='recipe-ingred_txt') if not (i.string is None or 'all ingredients' in i.string)]
    instructions = [str(i.string).strip() for i in soup.find_all(class_='recipe-directions__list--item') if i.string is not None]
    try:
        time = [str(i['aria-label']) for i in soup.find_all(class_='prepTime__item') if i.has_attr('aria-label')]
    except AttributeError:
        time = []

    try:
        notes = [str(k.string) for k in soup.find(class_='recipe-footnotes').find_all('li')]
    except AttributeError:
        notes = []

    try:
        stars = float(soup.find(class_='rating-stars')['data-ratingstars'])
    except KeyError:
        return None, False, 'nostars', set(), id

    scraped_ids = set()
    for a in soup.find_all('a'):
        if a.has_attr('href') and a['href'].startswith('https://www.allrecipes.com/recipe/'):
            scr_id = a['href'].replace('https://www.allrecipes.com/recipe/', '')
            scr_id = int(scr_id[:scr_id.find('/')])
            scraped_ids.add(scr_id)

    nutrition = {}
    try:
        for k in soup.find(class_='nutrition-summary-facts').children:
            if isinstance(k, NavigableString):
                continue
            if k.has_attr('itemprop'):
                if k['itemprop'] == 'calories':
                    nutrition[k['itemprop']] = (tryfloat(k.string.replace('calories', '').replace(';', '').strip()), 'calories')
                else:
                    nutrition[k['itemprop']] = (tryfloat(k.contents[0].strip()), str(k.span['aria-label'].replace(';', '')))
    except AttributeError:
        pass

    recipe = {'id': id, 'name': name, 'stars': stars, 'ingredients': ingredients, 'instructions': instructions, 'time': time, 'notes': notes, 'nutrition': nutrition}

    if r.status_code == 200:
        with open(html_path / f'{id}.html', 'w+') as f:
            f.write(r.text)
    return recipe, True, r.status_code, scraped_ids, id


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def get_batch(max_batch_size):
    steps = 1
    global high_priority_queue
    global queue

    while True:
        batch_size = min(steps, max_batch_size)
        if len(high_priority_queue) >= batch_size:
            sample = random.sample(high_priority_queue, batch_size)
            yield sample
        else:
            sample = random.sample(queue, batch_size)
            yield sample
        steps += 1

pickle_path = Path('recipe_data.pickle')
html_path = Path('raw_html_files')

if not html_path.exists():
    os.mkdir(html_path)

recipes = []
done_ids = {}
queue = set(range(0, 4000000)) - set(done_ids.keys())
high_priority_queue = {255220}
if pickle_path.exists():
    with open(pickle_path, 'rb') as f:
        x = pickle.load(f)
        recipes = x['recipes']
        done_ids = x['done_ids']
        queue = x['queue']
        high_priority_queue = x['high_priority_queue']

# asynchronous implementation
p = Pool(2)
last_saved = time()

for id_batch in get_batch(32):
    results = p.map(get_recipe, id_batch)

    high_priority_queue = high_priority_queue - set(id_batch)
    queue = queue - set(id_batch)

    for result in results:
        recipe, success, status_code, scraped_ids, id = result
        high_priority_queue.update(scraped_ids-set(done_ids.keys()))
        print(id, status_code, recipe['name'] if success else '')

        if success:
            recipes.append(recipe)

        done_ids[id] = (success, status_code)

    if time()-last_saved > 30:
        print('# Saved!')
        last_saved = time()
        with open(pickle_path, 'wb') as f:
            pickle.dump({'recipes': recipes, 'done_ids': done_ids, 'high_priority_queue': high_priority_queue, 'queue': queue}, f)

    sleep(0.5)


"""p = Pool(10)
last_saved = time()
for id_range in chunks(queue, 100):
    results = p.map(get_recipe, id_range)
    for result in results:
        html, recipe = result
        print(recipe['name'])

        with open(html_path/f'{id}.html', 'w+') as f:
            f.write(html)
            recipes.append(recipe)
    if time()-last_saved > 60*2:
        last_saved = time()
        with open(pickle_path, 'wb') as f:
            pickle.dump(recipes, f)"""


#_, _, _, _, ids = get_recipe(220000)
#print(ids)
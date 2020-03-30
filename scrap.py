# Parameters

# Wikiaves author_id range for scraping all its registries.
USER_ID_RANGE = (1, 10)

# Number of users to be sampled in USER_ID_RANGE. Set to zero for no sampling.
SAMPLE_USER_ID = 0

# Set this to true for append the author hometown on each registry
APPEND_AUTHOR_HOMETOWN = True

# Path for the pickle output
OUTPUT_PATH = "wikiaves_authors_{}-{}.pkl".format(USER_ID_RANGE[0], USER_ID_RANGE[1])


# Dependences

import requests as req
import json
import pandas as pd
import numpy as np
import random
from pandarallel import pandarallel
from bs4 import BeautifulSoup
import time
from tqdm import tqdm


# Initialize TQDM and Pandarallel

tqdm.pandas()
pandarallel.initialize(progress_bar=True)

# Scraping parameters

URI = "https://www.wikiaves.com.br/getRegistrosJSON.php"
params = {"tm": "f",
  "t": "u",
  "o": "dp",
  "desc": 1,
  "p": 10}

if SAMPLE_USER_ID > 0:
    user_ids = pd.Series(random.sample(range(1, USER_ID_RANGE[1]), SAMPLE_USER_ID))
else:
    user_ids = pd.Series([i for i in range(*USER_ID_RANGE)])

# Shuffle
user_ids = user_ids.reindex(np.random.permutation(user_ids.index))


# Scraping functions

def get_user_registries(user_id: int) -> pd.DataFrame:
    """
    Gets an DataFrame containing all observation registries for an
    given user_id.
    """
    run_flag = True
    pag_num = 1
    user_data = []
    while run_flag:
        params["u"] = user_id
        params["p"] = pag_num
        r = req.get(URI, params)
        try:
            raw_data = json.loads(r.text)
        except json.JSONDecodeError:
            break
        pag_data = pd.DataFrame(raw_data["registros"]["itens"]).T
        if (pag_data.size < 1):
            run_flag = False
        user_data.append(pag_data)
        pag_num += 1
    if len(user_data) > 0:
        return pd.concat(user_data).assign(user_id=user_id)
    else:
        return None


def get_profile_hometown(profile: str) -> str:
    """
    Gets the author hometown for an given profile identifier.
    """
    profile_URI = "https://www.wikiaves.com.br/perfil_{}".format(profile)
    r = req.get(profile_URI)
    soup = BeautifulSoup(r.text, 'html.parser')
    try:
        id_municipio = soup.findAll("a", {"class": 'm-card-profile__email m-link'})[-1]["href"].split("?c=")[-1]
    except:
        id_municipio = None
    return id_municipio


# The action

time1 = time.time()
data = (pd.concat(user_ids.progress_apply(get_user_registries).tolist())
          .set_index("user_id"))
time2 = time.time()
print(time2 - time1)
data.to_pickle(OUTPUT_PATH)


# Append author hometowns, optional

if APPEND_AUTHOR_HOMETOWN:
    profiles = (pd.DataFrame(data.perfil.unique())
              .rename(columns={0: "perfil"})
              .assign(hometown_ids=lambda series: series.parallel_applymap(get_profile_hometown))
              .set_index("perfil"))
    final_data = data.join(profiles, on="perfil")
    final_data.to_pickle(OUTPUT_PATH)
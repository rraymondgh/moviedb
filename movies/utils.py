import requests, json, re, urllib.parse, threading
from bs4 import BeautifulSoup
import pandas as pd
import wget,gzip, yaml
from pathlib import Path
import numpy as np
import datetime as dt, time

class util():
    @staticmethod
    # more granular view of data in columns...
    def catcolumns(df):
        obj = {c:str(t) for c,t in df.dtypes.to_dict().items()}
        for c in obj.keys():
            if obj[c]=="object":
                obj[c] = "list" if df[c].dropna().apply(lambda x: isinstance(x, list)).any() else obj[c]
                try:
                    if obj[c] == "object":
                        obj[c] = "url" if df[c].str.contains("http").any() else "string"
                    if obj[c] == "string":
                        obj[c] = "usd" if df[c].str.contains("^\$[0-9,]*$").any() else obj[c]
                    if obj[c] == "string":
                        obj[c] = "artwork" if df[c].str.contains("^/.*\.jpg$").any() else obj[c]
                except AttributeError:
                    pass

        return pd.DataFrame([obj]).T.rename(columns={0:"type"})


class imdb():
    @staticmethod
    def getdata(path=Path().cwd()):
        resp = requests.get("https://datasets.imdbws.com")
        soup = BeautifulSoup(resp.content.decode(), "html.parser")
        files = {}
        for f in soup.find_all("a", href=True):
            if f["href"].endswith('gz'):
                u = urllib.parse.urlparse(f["href"])
                fn = path.joinpath(u.path.strip("/"))
                files[Path(fn.stem).stem] = fn
                if not fn.is_file():
                    wget.download(f["href"], out=path.as_posix())

        return files

    @staticmethod
    def normalise(files, alldata=False, subsetdata=True):
        dfs={}

        # work with a subset of data to speed up modelling and iterations.  Take a few major actors and titles
        # as criteria to build a manageable representative set of data
        l = ["Tom Hanks","Will Smith","Clint Eastwood","Leonardo DiCaprio","Johnny Depp","Meryl Streep","Bruce Willis"]
        k = "name.basics"
        with open(files[k].parent.joinpath("wanted.json")) as f: tm = json.load(f)

        # work with subset for modelling purpose
        if alldata:
            dfs[k] = pd.read_csv(gzip.open(files[k]), sep="\t").replace({"\\N":np.nan})
            if subsetdata:
                # manage down size of nmi
                mask = dfs[k]["primaryName"].isin(l).fillna(False)
                for t in tm:
                    mask = mask | dfs[k]["knownForTitles"].str.contains(t["tconst"]).fillna(False)
                mask = mask & dfs[k]["knownForTitles"].str.contains("tt").fillna(False)
                dfs[k] = dfs[k].loc[mask,]
                dfs[k].to_csv(f"{files[k]}_subset.tsv", sep="\t", index=False)
        else:
            dfs[k] = pd.read_csv(f"{files[k]}_subset.tsv", sep="\t")
        dfs[k] = dfs[k].astype({c:"Int64" for c in dfs[k].columns}, errors="ignore")
        # birth year is a lot but getting data issues...
        # dfs[k] = dfs[k].dropna(subset=["primaryProfession","birthYear"])

        # comma separated - not good for joins and merges. rename for consistency
        dfs["nmi"] = (dfs["name.basics"].loc[:,["nconst","knownForTitles"]]
        .assign(knownForTitles=lambda x: x["knownForTitles"].str.split(","))
        .explode("knownForTitles")
        ).rename(columns={"knownForTitles":"tconst"}).drop_duplicates()
        # already extracted known titles so can drop and de-dup - e.g. Tom Hanks
        dfs[k] = dfs[k].drop(columns=["knownForTitles"]).drop_duplicates()

        for k in [k for k in files.keys() if k not in ["name.basics","omdb.titles"]]:
            if alldata:
                dfs[k] = pd.read_csv(gzip.open(files[k]), sep="\t").replace({"\\N":np.nan})
                if k=="title.akas": dfs[k]=dfs[k].rename(columns={"titleId":"tconst"})
                # subset titles to those we have names
                if subsetdata:
                    c = "tconst" if k!= "title.episode" else "parentTconst"
                    try:
                        (dfs[k].loc[dfs[k][c].isin(dfs["nmi"]["tconst"])]
                        .to_csv(f"{files[k]}_subset.tsv", sep="\t", index=False))
                    except KeyError as e:
                        print(k, dfs[k].columns, e)
            else:
                dfs[k] = pd.read_csv(f"{files[k]}_subset.tsv", sep="\t")
            dfs[k] = dfs[k].astype({c:"Int64" for c in dfs[k].columns}, errors="ignore")

        dfs["name.and.titles"] = dfs["nmi"].merge(dfs["name.basics"], on="nconst").merge(dfs["title.basics"], on="tconst")
        return dfs

class omdb():
    @staticmethod
    def getandnormalise(apikey, dfs={}, files={}, path=Path().cwd(), limit=500):
        omdbcols = ['Title', 'Year', 'Rated', 'Released', 'Runtime', 'Genre', 'Director', 'Writer', 'Actors', 'Plot', 'Language', 'Country', 'Awards', 'Poster', 'Ratings', 'Metascore', 'imdbRating', 'imdbVotes', 'imdbID', 'Type', 'DVD', 'BoxOffice', 'Production', 'Website', 'Response']
        omdbk = "omdb.titles"
        files[omdbk] = path.joinpath(f"{omdbk}.json")
        if not path.joinpath(files[omdbk]).is_file():
            dfs[omdbk] = pd.DataFrame(columns=omdbcols)
        else:
            dfs[omdbk] = pd.read_json(path.joinpath(files[omdbk]))
            dfs[omdbk] = dfs[omdbk].astype({c:"Int64" for c in dfs[omdbk].columns}, errors="ignore")
            if limit==0:
                return dfs, files
            

        k = "title.basics"
        c=0
        # limited to 1000 API calls a day, so only fetch if have not done already
        for tconst in dfs[k].loc[~(dfs[k]["tconst"].isin(dfs[omdbk]["imdbID"]))]["tconst"].values:
            c+=1
            if c>limit:
                break
            # tt0109830	movie	Forrest Gump
            # http://www.omdbapi.com/?i=tt3896198&apikey=xxx
            params={"apikey":apikey,"i":tconst,"plot":"full"}
            res = requests.get("http://www.omdbapi.com/", params=params)
            if res.status_code!=200:
                print("OMDB breached API limit")
                break
            else:
                dfs[omdbk] = pd.concat([dfs[omdbk], pd.json_normalize(res.json())])

        if limit>0:
            print(f"OMdB sourced: {c}")
        # cleanup any bad data from concats and API calls
        dfs[omdbk] = dfs[omdbk].dropna(subset=["Title","imdbID"]).reset_index(drop=True)
        # convert text based values to numeric
        for col in util.catcolumns(dfs[omdbk]).query("type=='usd'").index:
            dfs[omdbk][col] = pd.to_numeric(dfs[omdbk][col].str.replace(r'[$,-]', ''), errors="ignore")#.astype("Int64")

        # split out the ratings as columns...
        dfr = (pd.json_normalize(dfs["omdb.titles"].loc[:,["imdbID","Ratings"]]
                        .explode("Ratings").to_dict(orient="records"))
            .drop(columns="Ratings")
            .dropna()
            .set_index(["imdbID","Ratings.Source"])
            .unstack()
        )
        dfr.columns = dfr.columns.droplevel()
        dfr = dfr.rename(columns={c:f"Rating_{c.replace(' ', '')}" for c in dfr.columns})
        dfs["omdb.titles"] = dfs["omdb.titles"].loc[:,[c for c in dfs["omdb.titles"] 
                    if "Rating_" not in c ]].merge(dfr.reset_index(), on="imdbID", how="left")

        dfs[omdbk].to_json(path.joinpath(files[omdbk]))
        return dfs, files

class tmdb():
    @staticmethod
    def getandnormalise(apikey, dfs={}, files={}, path=Path().cwd(), limit=100):
        tmdbzip = list(path.glob("movie_ids_*.json.gz"))
        if len(tmdbzip)==0:
            wget.download(f'http://files.tmdb.org/p/exports/movie_ids_{(dt.datetime.today() - dt.timedelta(days=1)).strftime("%m_%d_%Y")}.json.gz',
            out=path.as_posix())
        tmdbzip = list(path.glob("movie_ids_*.json.gz"))
        tmdbraw = pd.read_json(gzip.open(tmdbzip[0]), lines=True)

        tmdbk = "tmdb.titles"
        # omdbk = "omdb.titles"

        files[tmdbk] = path.joinpath(f"{tmdbk}.json")
        if not path.joinpath(files[tmdbk]).is_file():
            dfs[tmdbk] = pd.DataFrame({"id":[]}).astype("int64")
        else:
            dfs[tmdbk] = pd.read_json(files[tmdbk]).reset_index(drop=True)
        if limit==0:
            return dfs, files

        # build a mapping between tmdb batch file and omdb data as candidate lookups
        subset = dfs["title.basics"].loc[:,["primaryTitle"]].merge(tmdbraw.loc[:,["id","original_title"]], 
                                        left_on="primaryTitle", 
                                        right_on="original_title", 
                                        how="left").dropna().astype({"id":"int64"}).drop_duplicates()

        params = {"api_key":apikey}
        c=0
        for id in subset.loc[~(subset["id"].isin(dfs[tmdbk]["id"].values))]["id"].unique():
            res = requests.get(f"https://api.themoviedb.org/3/movie/{id}", params=params)
            if res.status_code==200:
                dfs[tmdbk] = pd.concat([dfs[tmdbk], pd.json_normalize(res.json())])
            else:
                print(f"TMDB bad response {id} {res.status_code} {res.content}")
                # break
            c += 1
            if c>limit:
                # print("TMdB take a break")
                break
        if limit>0:
            print(f"TMDB sourced: {c}")
        dfs[tmdbk] = dfs[tmdbk].reset_index(drop=True)
        dfs[tmdbk].to_json(files[tmdbk])
        return dfs, files

class apple():
    @staticmethod
    def getandnormalise(dfs={}, files={}, path=Path().cwd(), limit=500, sleep=3):
        applek = "titles.apple"
        files[applek] = path.joinpath(f"{applek}.json")
        if files[applek].is_file():
            dfs[applek] = pd.read_json(files[applek])
            dfs[applek]["captured"] = pd.to_datetime(dfs[applek]["captured"], unit="ms")
        else:
            dfs[applek] = pd.DataFrame()
        if limit==0:
            return dfs,files

        # just get titles we don't have apple data for and are movies
        # subset = dfs["omdb.titles"].loc[(~dfs["omdb.titles"]["imdbID"].isin(dfs[applek]["tconst"]))&(dfs["omdb.titles"]["Type"]=="movie")]
        subset = dfs["title.basics"].loc[(~dfs["title.basics"]["tconst"].isin(dfs[applek]["tconst"]))&(dfs["title.basics"]["titleType"]=="movie")]

        c=0
        e=0
        now = dt.datetime.now()
        for n in subset.loc[:,["tconst","primaryTitle"]].values:
            c+=1
            if c>limit:
                # print(f"apple take a break, errors:{e}")
                break
            if c%100==0:
                dfs[applek] = dfs[applek].assign(strongmatch=lambda x: x["name"]==x["trackName"]).reset_index(drop=True)
                dfs[applek].to_json(files[applek])
                print(f"apple batch {c} errors:{e}")
            ip = {"term":n[1], "explicit":"Y","media":"movie","country":"us"}
            res = requests.get("https://itunes.apple.com/search", params=ip)
            if res.status_code==200:
                # append columns that allow join/name match
                s = {"tconst":n[0], "name":n[1], "search":res.url, "captured":dt.datetime(now.year, now.month, now.day, now.hour)}
                if res.json()["resultCount"] > 0:
                    dfs[applek] = pd.concat([dfs[applek], pd.json_normalize(res.json()["results"]).assign(**s)])
                else:
                    # append data so we don't search again...
                    s["trackName"] = "not found"
                    dfs[applek] = pd.concat([dfs[applek], pd.DataFrame([s])])
            else:
                e+=1
                if e%10 == 0:
                    print(f"nasty apple {res.status_code} {res.url}")
            time.sleep(sleep)  # apple allow 20 calls per min, backoff to see if get fewer 404 errors

        if limit>0:
            print(f"apple - sourced: {c}, errors: {e}")

        # add a column where serached title and returned title are the same    
        dfs[applek] = dfs[applek].assign(strongmatch=lambda x: x["name"]==x["trackName"]).reset_index(drop=True)
        dfs[applek].to_json(files[applek])
        return dfs, files

class mojo():
    @staticmethod
    def soup(tconst, now = dt.datetime.now()):
        params = {"ref_":"bo_se_r_1"}
        res = requests.get(f"https://www.boxofficemojo.com/title/{tconst}", params=params)
        if res.status_code!=200:
            return pd.DataFrame()
        # append columns that allow join/name match
        s = {"tconst":tconst, "search":res.url, "captured":dt.datetime(now.year, now.month, now.day, now.hour)}
        soup = BeautifulSoup(res.content.decode(), "html.parser")
        # extract the table using pandas.... make sure mandatory structure is in place...
        if Path(urllib.parse.urlparse(res.url).path).stem == tconst:
            try:
                # take all the tables... standardise the columns a bit
                dft = pd.concat([df.rename(columns={"Area":"Release Group", "Release Date":"Rollout", "Gross":"Domestic"}) 
                                 for df in pd.read_html(str(soup))])

                if ("Release Group" not in dft.columns.values) or ("Original Release" not in dft["Release Group"].values):
                    dft = pd.concat([dft, pd.DataFrame([{"Release Group":"Original Release"}])])
            except (IndexError, ImportError):
                dft = pd.DataFrame([{"Release Group":"Original Release"}])
        else:
            dft = pd.DataFrame([{"Release Group":"Original Release"}])

        data = []
        try:
            # get the part of the page that has heading "All Releases"
            boxo = soup.find(lambda tag: tag.name=="h2" and "All Releases" in tag.text).parent
            for d in boxo.select("div"):
                span = d.select("span")
                k = re.sub(r"^([A-Za-z]+).*$", r"\1", span[0].text.strip())
                data.append({"name":f'All{k}', "val":span[len(span)-1].text.strip()})
            # get the part of the page that looks like a table of data
            boxo = soup.find(text='Earliest Release Date').parent.parent.parent.find_all("div")
            for d in boxo:
                vals = d.find_all("span")
                if len(vals)>=2:
                    n = vals[0].text.replace("\n", "").strip()
                    v = re.sub("[ ]+", " ", vals[1].text.replace("\n","")).strip()
                    if n=="Domestic Distributor": v=v.replace("See full company information","")
                    if n!="IMDbPro":
                        data.append({"name":n, "val":v})
        except AttributeError:
            data.append({"name":"Domestic Distributor", "val":"not found"})

        # just add details to original release
        dft = dft.merge(pd.DataFrame(data).set_index("name").T.assign(**{"Release Group":"Original Release"}), 
                        on="Release Group", how="left").assign(**s)

        # remove the pesky unicode character that is a dash...
        dft.replace({"\u2013":np.nan}, inplace=True)
        for col in util.catcolumns(dft).query("type=='usd'").index:
            dft[col] = pd.to_numeric(dft[col].str.replace(r'[$,-]', ''), errors="ignore").astype("Int64", errors="ignore")
        return dft



    @staticmethod
    def getandnormalise(dfs={}, files={}, path=Path().cwd(), limit=500):
        mojok = "mojo.boxoffice"
        files[mojok] = path.joinpath(f"{mojok}.json")
        if files[mojok].is_file():
            dfs[mojok] = pd.read_json(files[mojok])
            dfs[mojok]["captured"] = pd.to_datetime(dfs[mojok]["captured"], unit="ms")
            dfs[mojok].replace({"\u2013":np.nan}, inplace=True)
            if limit==0:
                return dfs, files
        else:
            dfs[mojok] = pd.DataFrame({"tconst":[]})

        # just get titles we don't have boxoffice data for and are movies
        subset = dfs["title.basics"].loc[(~dfs["title.basics"]["tconst"].isin(dfs[mojok]["tconst"]))&(dfs["title.basics"]["titleType"]=="movie")]

        c=0
        now = dt.datetime.now()
        # for n in subset.loc[:,["imdbID","Title"]].values:
        for n in subset.loc[:,["tconst","primaryTitle"]].values:
            c+=1
            if c>limit:
                break
            # don't loose everything if mojo denies access for over hitting website...
            if c%100==0:
                print(f"mojo batch {c}")
                dfs[mojok].reset_index(drop=True).to_json(files[mojok])

            try:
                dfs[mojok] = pd.concat([dfs[mojok], mojo.soup(n[0], now=now)])
            except Exception as e:
                print(n[0])
                raise

        print(f"mojo take a break. sourced: {c}")

        dfs[mojok].reset_index(drop=True).to_json(files[mojok])
        return dfs, files

def main():
    p = Path().cwd().joinpath("jupyter").joinpath("data")
    l = 2000
    al = l//2
    print("hello")
    with open ("apikeys.yaml") as f: keys = yaml.safe_load(f)
    files = imdb.getdata(p)
    # regen subset...
    if False:
        dfs = imdb.normalise(files, alldata=True, subsetdata=True)
    dfs = imdb.normalise(files, alldata=False, subsetdata=True)
    # dfs, files = omdb.getandnormalise(keys["keys"]["omdb"], dfs, files, path=p, limit=l)
    # dfs, files = tmdb.getandnormalise(keys["keys"]["tmdb"], dfs, files, path=p, limit=l)
    # dfs, files = apple.getandnormalise(dfs, files, path=p, limit=l, sleep=4)
    # dfs, files = mojo.getandnormalise(dfs, files, path=p, limit=l)

    # use threading to concurrently go to sources...
    t1 = threading.Thread(name="omdb", target=omdb.getandnormalise, args=(keys["keys"]["omdb"], dfs, files), kwargs={"path":p, "limit":l})
    t2 = threading.Thread(name="tmdb", target=tmdb.getandnormalise, args=(keys["keys"]["tmdb"], dfs, files), kwargs={"path":p, "limit":l})
    t3 = threading.Thread(name="apple", target=apple.getandnormalise, args=(dfs, files), kwargs={"path":p, "limit":al, "sleep":3})
    t4 = threading.Thread(name="mojo", target=mojo.getandnormalise, args=(dfs, files), kwargs={"path":p, "limit":l})
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    print("done")


if __name__ == '__main__':
    main()

## SWOOPA SCRAPER PROTOTYPE

Checks new listings in marketplaces.

### REQUIREMENTS

- Python >= 3.8
- Chrome Browser (for craiglist)

### INSTALLATION

```
pip install -r requiremnts.txt
```

### USAGE

**For Offerup**

```
 python3 ./src/offerup.py -q iphone
```

**For Craiglist**

```
 python3 ./src/craiglist.py -q iphone
```

Other argumrents for location, search distance can be added. More info can be found from

```
 python3 ./src/craiglist.py -h
```

- **q**: Search query
- **la**: Latitude of the search location.
- **lo**: Longitude of the search location.
- **d**: Search distance or circle

Default latitude and longitude is that of New York

Running the scripts will create a data folder that contains csv file acting as a database for the marketplace listings and new listings.

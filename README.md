# abondance: Python library for Internet Health Report API

## Installation

### The easy way 
``````
pip install abondance
``````


### From source files
Get the latest source files:
```
git clone git@github.com:InternetHealthReport/abondance.git
```

Install dependencies and install abondance:
```
cd abondance
sudo pip install -r requirements.txt 
sudo python setup.py install
```
## AS inter-dependency (AS hegemony)
### Example: Retrieve dependencies for AS2501 on September 15th, 2018
```python
from ihr.hegemony import Hegemony

hege = Hegemony(originasns=[2501], start="2018-09-15 00:00", end="2018-09-15 23:59")

for r in hege.get_results():
  print(r)
```

### Example: Retrieve dependents of AS2500 on September 15th, 2018
```python
from ihr.hegemony import Hegemony

hege = Hegemony(asns=[2500], start="2018-09-15 00:00", end="2018-09-15 23:59")

for r in hege.get_results():
  # Skip results from the global graph
  if r["originasn"] == 0:
    continue
  print(r)
```
## AS Delay
### Example: Retrieve delay for AS7922 on September 15th, 2018
```python
from ihr.delay import Delay

res = Delay(asns=[7922], start="2018-09-15 00:00", end="2018-9-15 23:59")

for r in res.get_results():
  print(r)
```

## AS Forwarding alarms
### Example: Retrieve forwarding alarms for AS7922 on September 15th, 2018
```python
from ihr.forwarding import Forwarding

res = Forwarding(asns=[7922], start="2018-09-15 00:00", end="2018-9-15 23:59")

for r in res.get_results():
  print(r)
```




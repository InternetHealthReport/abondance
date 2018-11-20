# Abondance
Python library for Internet Health Report API

### Example: Retrieve dependencies for AS2501 on September 15th, 2018
```
from abondance import Hegemony
hege = Hegemony(originasns=[2501], start="2018-09-15", end="2018-10-16")

for r in hege.get_results():
  print(r)
```

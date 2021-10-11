## Ntiles
Ntiles is a fast and lightweight quantile backtester aiming to measure the monotonic relationship between an
equity factor and future returns. The simplistic but powerful API allows users to load pricing data and test a factors 
in less than 5 lines of code. The packages utilize equity-db[LINK] to query and cache pricing data behind the scenes 
allowing users to solely focus on factor research.

### Why Ntiles?
For the past few years I have been a user of alphalens it's a great package however, alphalens has various 
shortcomings. Ntiles addresses those shortcomings, adds new useful functionality, improves code efficiency, and 
generates more accurate backtests. 


1) Speed! Ntiles is over 6 times faster than alphalens when generating tearsheets.
2) Accurate cumulative returns! Each day Ntiles will compute the daily returns of each quantile using daily asset
   returns and weights.
    1) This is intuitively simple but something alphalens does not do for holding period's greater than 1 day.
3) Users don't need to worry about reading and formatting pricing data. Ntiles has *Portals* which query and cache pricing data using
   equity-db[LINK].
4) No more pesky time zone errors! Ntiles uses pandas Periods.

```python
from ntiles import Ntile, PricingPortal, SectorPortal

# getting the asset pricing data
pricing_portal = PricingPortal(assets=my_universe, start='2017-01-01', end='2021-01-01')
# getting the group data, this is optional
group_portal = SectorPortal(assets=my_universe)

# generating tearsheets
tile = Ntile(pricing_portal=pricing_portal, group_portal=group_portal)
tile.full_tear(factor=my_factor, ntiles=5, holding_period=20)
```

### Example Tearsheet
![img.png](test/img.png)
![img_1.png](test/img_1.png)
![img_2.png](test/img_2.png)
![img_3.png](test/img_3.png)
![img_4.png](test/img_4.png)
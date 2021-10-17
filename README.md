## Ntiles
Ntiles is a fast and lightweight quantile backtester. 
Ntiles has a simplistic but powerful API which allows users to load pricing data and test the predictive ability of factors in less than 5 lines of code. 
The package utilizes [equity-db](https://github.com/Alexd14/equity-db) to query and cache pricing data behind the scenes.

### Why Ntiles?
1) Speed! Ntiles is over 6 times faster than alphalens when generating tearsheets.
2) Accurate cumulative returns! Each day Ntiles will compute the daily returns of each quantile using daily asset
   returns and weights.
    1) This is intuitively simple but something alphalens does not do for holding period's greater than 1 day.
3) Users don't need to worry about reading and formatting pricing data. Ntiles has *Portals* which query and cache 
pricing data using [equity-db](https://github.com/Alexd14/equity-db).
4) No more pesky time zone errors! Ntiles uses Pandas Periods.

### API
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
![](ntiles/examples/inspection_1.png)
![](ntiles/examples/inspection_2.png)
![](ntiles/examples/return_1.png)
![](ntiles/examples/return_2.png)
![](ntiles/examples/ic_ac.png)
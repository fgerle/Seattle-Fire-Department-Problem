from sklearn import linear_model
import numpy as np

class populationPredictor:

    def __init__(self):
        self.population ={2024: 3549000, 2023: 3519000, 2022: 3489000, 2021: 3461000, 2020: 3433000,
                     2019: 3406000, 2018: 3379000, 2017: 3339000, 2016: 3299000, 2015: 3259000,
                     2014: 3220000, 2013: 3182000, 2012: 3143000, 2011: 3106000, 2010: 3069000,
                     2009: 3032000, 2008: 2996000, 2007: 2960000, 2006: 2924000, 2005: 2889000,
                     2004: 2855000, 2003: 2820000, 2002: 2787000, 2001: 2753000, 2000: 2720000,
                     1999: 2669000, 1998: 2613000, 1997: 2559000, 1996: 2505000, 1995: 2453000,
                     1994: 2401000, 1993: 2351000, 1992: 2302000, 1991: 2253000, 1990: 2206000,
                     1989: 2160000, 1988: 2114000, 1987: 2069000, 1986: 2025000, 1985: 1982000,
                     1984: 1940000, 1983: 1899000, 1982: 1858000, 1981: 1819000, 1980: 1780000,
                     1979: 1753000, 1978: 1730000, 1977: 1707000, 1976: 1685000, 1975: 1663000,
                     1974: 1641000, 1973: 1619000, 1972: 1598000, 1971: 1577000, 1970: 1556000,
                     1969: 1509000, 1968: 1456000, 1967: 1404000, 1966: 1354000, 1965: 1305000,
                     1964: 1259000, 1963: 1214000, 1962: 1171000, 1961: 1129000, 1960: 1089000,
                     1959: 1054000, 1958: 1021000}
        
        
        rates_y = []
        rates_x = []
        for k in self.population.keys():
            if k-1 in self.population.keys():
                rates_y.append(self.population[k]/self.population[k-1])
                rates_x.append([k])
        rX = np.array(rates_x)
        rY = np.array(rates_y)
        self.regr = linear_model.LinearRegression()
        self.regr.fit(rX, rY)

    def predict(self, year):
        pop = 0
        if year in self.population.keys():
            pop = self.population[year]
        else:
            last_date = max(self.population.keys())
            future = np.array(range(last_date, year), ndmin=2).transpose()
            fut_growth = self.regr.predict(future)
            pop = self.population[2024]
            i = 0
            for y in range(last_date, year):
                pop = pop*fut_growth[i]
                i += 1
        return(int(pop))
                
                

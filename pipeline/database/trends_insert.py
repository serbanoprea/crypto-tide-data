import luigi

from pipeline.common.read import read_sql_df
from pipeline.common.tasks import DatabaseQuery, TruncateTableQuery
from pipeline.database.database_population import DatabaseHourly, DatabaseDaily

_config = luigi.configuration.get_config()
_values_table = _config.get('database', 'values-table')
_hourly_trends_table = _config.get('database', 'hourly-trends-table')
_daily_trends_table = _config.get('database', 'daily-trends-table')


class EmptyHourlyTrends(TruncateTableQuery):
    date_hour = luigi.DateHourParameter()
    table = _hourly_trends_table

    def requires(self):
        return DatabaseHourly(date_hour=self.date_hour)


class InsertHourlyTrends(DatabaseQuery):
    date_hour = luigi.DateHourParameter()

    def requires(self):
        return EmptyHourlyTrends(date_hour=self.date_hour)

    sql_template = """
         WITH InitialTable AS (
                 SELECT 
                     Symbol,
                     Date,
                     Hour,
                     Price,
                     Volume
                 FROM {values_table}
                 WHERE Price != 0 AND Symbol='{coin}' 
            ),
            Differences AS (
                SELECT
                    InitialTable.*,
                    LAG(Price) OVER (PARTITION BY Symbol ORDER BY Date, Hour) AS PreviousPrice
                FROM InitialTable
            ),
            Groups AS ( -- Represents consecutive increases
                SELECT
                    Differences.*,
                    SUM(CASE WHEN Price < PreviousPrice OR Price = PreviousPrice THEN 1 ELSE 0 END) OVER (ORDER BY Date, Hour) AS HourlyPriceGroup
                FROM Differences
            ),
            Behavours AS (
                SELECT
                    Groups.*,
                    ROW_NUMBER() OVER (PARTITION BY HourlyPriceGroup ORDER BY Date, Hour) AS ConsecutiveIncreases,
                    (
                        ABS(MAX(Price) OVER (PARTITION BY HourlyPriceGroup ORDER BY Date, Hour) - MIN(Price) OVER (PARTITION BY HourlyPriceGroup ORDER BY Date, Hour)) /
                        (MAX(Price) OVER (PARTITION BY HourlyPriceGroup ORDER BY Date, Hour) + MIN(Price) OVER (PARTITION BY HourlyPriceGroup ORDER BY Date, Hour)/2)
                    ) AS PercentageIncrease,
                    (Price / PreviousPrice * 100 - 100) AS HourlyChange
                FROM Groups
            )
            
            INSERT INTO {hourly_trends_table}
            SELECT 
                Symbol,
                Date,
                Hour,
                Price,
                ConsecutiveIncreases,
                PercentageIncrease,
                HourlyChange
            FROM Behavours
            WHERE HourlyChange IS NOT NULL;    
    """

    @property
    def sql(self):
        df = read_sql_df(['coin_id', 'previous_rank', 'rank', 'symbol'])
        for coin in df['symbol'].values:
            yield self.sql_template.format(coin=coin, hourly_trends_table=_hourly_trends_table, values_table=_values_table)


class EmptyDailyTrends(TruncateTableQuery):
    date = luigi.DateParameter()
    table = _daily_trends_table

    def requires(self):
        return DatabaseDaily(date=self.date)


class InsertDailyTrends(DatabaseQuery):
    date = luigi.DateParameter()

    def requires(self):
        return EmptyDailyTrends(date=self.date)

    sql_template = """
     WITH PriceHistogram AS (
            SELECT DISTINCT
                Symbol AS Coin,
                Date AS GroupDate,
                PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY Price) OVER (PARTITION BY Date) AS Price25Perc,
                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY Price) OVER (PARTITION BY Date) AS PriceMedian,
                PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY Price) OVER (PARTITION BY Date) AS Price75Perc
            FROM [dbo].[Values]
            WHERE Price != 0 AND Symbol='{coin}'
        ),
        InitialTable AS (
             SELECT 
                 Symbol,
                 [Date],
                 MAX(Price) AS MaxPrice,
                 MIN(Price) AS MinPrice
             FROM {values_table}
             WHERE Price != 0 AND Symbol='{coin}'
             GROUP BY Symbol, [Date]
        ),
        CompleteInitTable AS (
            SELECT
                Symbol,
                Date,
                MaxPrice,
                MinPrice,
                Price25Perc,
                PriceMedian,
                Price75Perc
            FROM InitialTable INNER JOIN PriceHistogram 
            ON InitialTable.Date = PriceHistogram.GroupDate AND InitialTable.Symbol = PriceHistogram.Coin
        ),
        Differences AS (
            SELECT
                CompleteInitTable.*,
                LAG(MinPrice) OVER (PARTITION BY Symbol ORDER BY Date) AS PreviousMin,
                LAG(Price25Perc)  OVER (PARTITION BY Symbol ORDER BY Date) AS Previous25Perc,
                LAG(PriceMedian) OVER (PARTITION BY Symbol ORDER BY Date) AS PreviousMedian,
                LAG(Price75Perc)  OVER (PARTITION BY Symbol ORDER BY Date) AS Previous75Perc,
                LAG(MaxPrice) OVER (PARTITION BY Symbol ORDER BY Date) AS PreviousMax
            FROM CompleteInitTable
        ),
        Groups AS ( -- Represents consecutive increases
            SELECT
                Differences.*,
                SUM(CASE WHEN PriceMedian < PreviousMedian THEN 1 ELSE 0 END) OVER (ORDER BY Date) AS SmallIncreaseGroup,
                SUM(CASE WHEN PriceMedian < Previous75Perc THEN 1 ELSE 0 END) OVER (ORDER BY Date) AS MediumIncreaseGroup,
                SUM(CASE WHEN PriceMedian < PreviousMax THEN 1 ELSE 0 END) OVER (ORDER BY Date) AS HighIncreaseGroup
            FROM Differences
            WHERE PreviousMin IS NOT NULL AND Previous75Perc IS NOT NULL AND PreviousMax IS NOT NULL AND PreviousMedian IS NOT NULL
        ),
        Behavours AS (
            SELECT
                Groups.*,
                (PriceMedian / PreviousMedian * 100 - 100) AS OverallChange,
                (MAX(PriceMedian) OVER (PARTITION BY SmallIncreaseGroup ORDER BY Date) / MIN(PreviousMedian) OVER (PARTITION BY SmallIncreaseGroup ORDER BY Date) * 100 - 100) AS SmallPercChange,
                (MAX(PriceMedian) OVER (PARTITION BY MediumIncreaseGroup ORDER BY Date) / MIN(Previous75Perc) OVER (PARTITION BY MediumIncreaseGroup ORDER BY Date) * 100 - 100) AS MediumPercChange,
                (MAX(PriceMedian) OVER (PARTITION BY HighIncreaseGroup ORDER BY Date) / MIN(PreviousMax) OVER (PARTITION BY HighIncreaseGroup ORDER BY Date) * 100 - 100) AS HighPercChange,
                ROW_NUMBER() OVER (PARTITION BY SmallIncreaseGroup ORDER BY Date) AS ConsecutiveSmallChange,
                ROW_NUMBER() OVER (PARTITION BY MediumIncreaseGroup ORDER BY Date) AS ConsecutiveMediumChange,
                ROW_NUMBER() OVER (PARTITION BY HighIncreaseGroup ORDER BY Date) AS ConsecutiveHighChange,		
                (PriceMedian / PreviousMedian * 100 - 100) AS MedianChange,
                (PriceMedian / Previous75Perc * 100 - 100) AS Median75PercChange,
                (PriceMedian / PreviousMax * 100 - 100) AS MedianMaxChange
            FROM Groups	
        )
        
        INSERT INTO {daily_trends_table}
        SELECT Symbol,
            Date,
            MinPrice,
            Price25Perc,
            PriceMedian,
            Price75Perc,
            MaxPrice,
            PreviousMin,
            Previous25Perc,
            PreviousMedian,
            Previous75Perc,
            PreviousMax,
            OverallChange,
            SmallPercChange,
            MediumPercChange,
            HighPercChange,
            MedianChange,
            Median75PercChange,
            MedianMaxChange,
            ConsecutiveSmallChange,
            ConsecutiveMediumChange,
            ConsecutiveHighChange
        FROM Behavours;    
    """

    @property
    def sql(self):
        df = read_sql_df(['coin_id', 'previous_rank', 'rank', 'symbol'])
        for coin in df['symbol'].values:
            yield self.sql_template.format(coin=coin, daily_trends_table=_daily_trends_table,
                                           values_table=_values_table)

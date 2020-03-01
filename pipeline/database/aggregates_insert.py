from datetime import datetime, time, timedelta

import luigi
from luigi.tools.range import RangeHourly

from pipeline.common.tasks import DatabaseQuery, TruncateTableQuery
from pipeline.database.trends_insert import InsertHourlyTrends

_config = luigi.configuration.get_config()
_population_aggregates = _config.get('database', 'population-aggregates-table')
_hourly_trends_table = _config.get('database', 'hourly-trends-table')
_coin_aggregates_table = _config.get('database', 'coin-aggregates-table')


class EmptyPopulationAggregates(TruncateTableQuery):
    date = luigi.DateHourParameter()
    table = _population_aggregates

    def requires(self):
        return RangeHourly(
            of=InsertHourlyTrends,
            start=datetime.combine(self.date, time.min),
            stop=datetime.combine(self.date + timedelta(days=1), time.min)
        )


class InsertPopulationAggregates(DatabaseQuery):
    date = luigi.DateParameter()

    def requires(self):
        return EmptyPopulationAggregates(**self.param_kwargs)

    @property
    def sql(self):
        return """
            WITH Initial AS (
                SELECT Date, SUM(OverallChange) AS SumChange, AVG(OverallChange) AS AverageChange, STDEV(OverallChange) AS StDevChange, 1 AS MockGroup
                FROM {hourly_trends}
                WHERE Price > 1
                GROUP BY Date
            ),
            Aggregations AS (
                SELECT Initial.*,
                    AVG(AverageChange) OVER(PARTITION BY MockGroup) AS OverallAverage,
                    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY AverageChange) OVER (PARTITION BY MockGroup) AS MedianChange,
                    (StDevChange / AverageChange) AS OverallScore
                FROM Initial
            ),
            PrepareData AS (
                SELECT Aggregations.*,
                    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY OverallScore) OVER (PARTITION BY MockGroup) AS Perc25Score,
                    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY OverallScore) OVER (PARTITION BY MockGroup) AS MedianScore,
                    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY OverallScore) OVER (PARTITION BY MockGroup) AS Perc75Score,
                    MAX(OverallScore) OVER (PARTITION BY MockGroup) AS MaxScore,
                    MIN(OverallScore) OVER (PARTITION BY MockGroup) AS MinScore
                FROM Aggregations
            )
            
            INSERT INTO {population_scores}
            SELECT
                Date,
                AverageChange,
                StDevChange,
                OverallScore,
                (CASE WHEN OverallScore > Perc25Score THEN 1 ELSE 0 END) AS HigherThan25Perc,
                (CASE WHEN OverallScore > MedianScore THEN 1 ELSE 0 END) AS HigherThanMedian,
                (CASE WHEN OverallScore > Perc75Score THEN 1 ELSE 0 END) AS HigherThan75Perc,
                (CASE WHEN OverallScore = MinScore THEN 1 ELSE 0 END) AS PopulationMinimum
            FROM PrepareData ORDER BY Date;
        """.format(hourly_trends=_hourly_trends_table, population_scores=_population_aggregates)


class EmptyCoinAggregates(TruncateTableQuery):
    date_hour = luigi.DateHourParameter()
    table = _coin_aggregates_table

    def requires(self):
        return InsertHourlyTrends(**self.param_kwargs)


class InsertCoinAggregates(DatabaseQuery):
    date_hour = luigi.DateHourParameter()

    def requires(self):
        return EmptyCoinAggregates(**self.param_kwargs)

    @property
    def sql(self):
        return """
                WITH DayPerformance AS (
                    SELECT
                        CoinId,
                        Symbol,
                        AVG(OverallChange) AS AverageDayChange,
                        SUM(OverallChange) AS SumDayChange,
                        STDEV(OverallChange) AS StDevDayChange,
                        COUNT(*) AS DayRecords
                    FROM {hourly_trends}
                    WHERE DATEADD(HOUR, Hour, CAST(Date as datetime)) >= DATEADD(HOUR, -24, GETDATE())
                    GROUP BY CoinId, Symbol
                ),
                WeekPerformance AS (	
                    SELECT
                        CoinId,
                        Symbol,
                        AVG(OverallChange) AS AverageWeekChange,
                        SUM(OverallChange) AS SumWeekChange,
                        STDEV(OverallChange) AS StDevWeekChange,
                        COUNT(*) AS WeekRecords,
                        1 AS MockGroup
                    FROM {hourly_trends}
                    WHERE DATEADD(HOUR, Hour, CAST(Date as datetime)) >= DATEADD(DAY, -7, GETDATE())
                    GROUP BY CoinId, Symbol
                ),
                PopulationAggregation AS (	
                SELECT
                    DayPerformance.*,
                    WeekPerformance.AverageWeekChange,
                    WeekPerformance.SumWeekChange,
                    WeekPerformance.WeekRecords,
                    WeekPerformance.StDevWeekChange,
                    (StDevWeekChange / AverageWeekChange) AS WeekVolatility,
                    (StDevDayChange / AverageDayChange) AS DayVolatility,
                
                    (CASE WHEN SumDayChange > PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY SumDayChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherThanMedianDay,
                    (CASE WHEN SumDayChange > PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY SumDayChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherThan25PercDay,
                    (CASE WHEN SumDayChange > PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY SumDayChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherThan75PercDay,	
                    (CASE WHEN SumDayChange > AVG(SumDayChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherThanAverageDay,
                
                    (CASE WHEN SumWeekChange > PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY SumWeekChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherThanMedianWeek,
                    (CASE WHEN SumWeekChange > PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY SumWeekChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherThan25PercWeek,
                    (CASE WHEN SumWeekChange > PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY SumWeekChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherThan75PercWeek,
                    (CASE WHEN SumWeekChange > AVG(SumWeekChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherThanAverageWeek,
                
                    (CASE WHEN StDevDayChange / AverageDayChange < PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY StDevDayChange / AverageDayChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherVolatilityThan75PercDay,
                    (CASE WHEN StDevDayChange / AverageDayChange < PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY StDevDayChange / AverageDayChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherVolatilityThan25PercDay,
                    (CASE WHEN StDevDayChange / AverageDayChange < PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY StDevDayChange / AverageDayChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherVolatilityThanMedianDay,	
                    (CASE WHEN StDevDayChange / AverageDayChange < AVG(StDevDayChange / AverageDayChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherThanAverageVolatilityDay,
                    (CASE WHEN StDevDayChange / AverageDayChange = MIN(StDevDayChange / AverageDayChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS MinDayVolatility,
                    (CASE WHEN StDevDayChange / AverageDayChange = MAX(StDevDayChange / AverageDayChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS MaxDayVolatility,
                
                    (CASE WHEN StDevWeekChange / AverageWeekChange < PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY StDevWeekChange / AverageWeekChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherVolatilityThan75PercWeek,
                    (CASE WHEN StDevWeekChange / AverageWeekChange < PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY StDevWeekChange / AverageWeekChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherVolatilityThan25PercWeek,
                    (CASE WHEN StDevWeekChange / AverageWeekChange < PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY StDevWeekChange / AverageWeekChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherVolatilityMedianWeek,
                    (CASE WHEN StDevWeekChange / AverageWeekChange < AVG(StDevWeekChange / AverageWeekChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS HigherThanAverageVolatilityWeek,
                    (CASE WHEN StDevWeekChange / AverageWeekChange = MIN(StDevWeekChange / AverageWeekChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS MinWeekVolatility,
                    (CASE WHEN StDevWeekChange / AverageWeekChange = MAX(StDevWeekChange / AverageWeekChange) OVER (PARTITION BY MockGroup) THEN 1 ELSE 0 END) AS MaxWeekVolatility
                FROM DayPerformance INNER JOIN WeekPerformance
                ON DayPerformance.CoinId = WeekPerformance.CoinId AND DayPerformance.Symbol = WeekPerformance.Symbol
                WHERE AverageWeekChange != 0 AND AverageDayChange != 0
                )
                
                INSERT INTO {coin_aggregates}
                SELECT
                    CoinId,
                    Symbol,
                
                    AverageDayChange,
                    SumDayChange,
                    StDevDayChange,
                    DayRecords,
                    AverageWeekChange,
                    SumWeekChange,
                    WeekRecords,
                    StDevWeekChange,
                    WeekVolatility,
                    DayVolatility,
                
                    HigherThanMedianDay,
                    HigherThan25PercDay,
                    HigherThan75PercDay,
                    HigherThanAverageDay,
                
                    HigherThanMedianWeek,
                    HigherThan25PercWeek,
                    HigherThan75PercWeek,
                    HigherThanAverageWeek,
                
                    HigherVolatilityThan75PercDay,
                    HigherVolatilityThan25PercDay,
                    HigherVolatilityThanMedianDay,
                    HigherThanAverageVolatilityDay,
                
                    HigherVolatilityMedianWeek,
                    HigherVolatilityThan75PercWeek,
                    HigherVolatilityThan25PercWeek,
                
                    HigherThanAverageVolatilityWeek,
                    MinWeekVolatility,
                    MaxWeekVolatility,
                    MinDayVolatility,
                    MaxDayVolatility
                FROM PopulationAggregation
                WHERE StDevDayChange IS NOT NULL;
        """.format(coin_aggregates=_coin_aggregates_table, hourly_trends=_hourly_trends_table)

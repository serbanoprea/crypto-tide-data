from datetime import datetime, time, timedelta

import luigi
from luigi.tools.range import RangeDaily

from pipeline.common.tasks import DatabaseQuery, TruncateTableQuery
from pipeline.database.trends_insert import InsertHourlyTrends


_config = luigi.configuration.get_config()
_population_scores = _config.get('database', 'population-scores-table')
_hourly_trends_table = _config.get('database', 'hourly-trends-table')


class EmptyHourlyTrends(TruncateTableQuery):
    date = luigi.DateHourParameter()
    table = _population_scores

    def requires(self):
        return RangeDaily(
            of=InsertHourlyTrends,
            start=datetime.combine(self.date, time.min),
            stop=datetime.combine(self.date + timedelta(days=1), time.min)
        )


class InsertPopulationScore(DatabaseQuery):
    date = luigi.DateParameter()

    def requires(self):
        return EmptyHourlyTrends(**self.param_kwargs)

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
                    (AverageChange / StDevChange) AS OverallScore
                FROM Initial
            ),
            PrepareData AS (
                SELECT Aggregations.*,
                    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY OverallScore) OVER (PARTITION BY MockGroup) AS Perc25Score,
                    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY OverallScore) OVER (PARTITION BY MockGroup) AS MedianScore,
                    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY OverallScore) OVER (PARTITION BY MockGroup) AS Perc75Score,
                    MAX(OverallScore) OVER (PARTITION BY MockGroup) AS MaxScore
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
                (CASE WHEN OverallScore > Perc75Score THEN 1 ELSE 0 END) AS HigherThan75Perc
            FROM PrepareData ORDER BY Date;
        """.format(hourly_trends=_hourly_trends_table, population_scores=_population_scores)

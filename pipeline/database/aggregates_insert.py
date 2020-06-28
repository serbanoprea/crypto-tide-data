# from datetime import datetime, time, timedelta
#
# import luigi
# from luigi.tools.range import RangeHourly
#
# from pipeline.common.tasks import DatabaseQuery, TruncateTableQuery
# from pipeline.database.database_population import InsertHourlyValues
#
# _config = luigi.configuration.get_config()
# _population_aggregates = _config.get('database', 'population-aggregates-table')
# _hourly_trends_table = _config.get('database', 'hourly-trends-table')
# _values_table = _config.get('database', 'values-table')
# _coin_aggregates_table = _config.get('database', 'coin-aggregates-table')
#
#
# # class EmptyPopulationAggregates(TruncateTableQuery):
# #     date = luigi.DateHourParameter()
# #     table = _population_aggregates
# #
# #     def requires(self):
# #         return RangeHourly(
# #             of=InsertHourlyTrends,
# #             start=datetime.combine(self.date, time.min),
# #             stop=datetime.combine(self.date + timedelta(days=1), time.min)
# #         )
#
#
# class InsertPopulationAggregates(DatabaseQuery):
#     date = luigi.DateParameter()
#
#     def requires(self):
#         return EmptyPopulationAggregates(**self.param_kwargs)
#
#     @property
#     def sql(self):
#         return """
#             WITH Initial AS (
#                 SELECT Date, SUM(OverallChange) AS SumChange, AVG(OverallChange) AS AverageChange, STDEV(OverallChange) AS StDevChange, 1 AS MockGroup
#                 FROM {hourly_trends}
#                 WHERE Price > 1
#                 GROUP BY Date
#             ),
#             Aggregations AS (
#                 SELECT Initial.*,
#                     AVG(AverageChange) OVER(PARTITION BY MockGroup) AS OverallAverage,
#                     PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY AverageChange) OVER (PARTITION BY MockGroup) AS MedianChange,
#                     (StDevChange / AverageChange) AS OverallScore
#                 FROM Initial
#             ),
#             PrepareData AS (
#                 SELECT Aggregations.*,
#                     PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY OverallScore) OVER (PARTITION BY MockGroup) AS Perc25Score,
#                     PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY OverallScore) OVER (PARTITION BY MockGroup) AS MedianScore,
#                     PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY OverallScore) OVER (PARTITION BY MockGroup) AS Perc75Score,
#                     MAX(OverallScore) OVER (PARTITION BY MockGroup) AS MaxScore,
#                     MIN(OverallScore) OVER (PARTITION BY MockGroup) AS MinScore
#                 FROM Aggregations
#             )
#
#             INSERT INTO {population_scores}
#             SELECT
#                 Date,
#                 AverageChange,
#                 StDevChange,
#                 OverallScore,
#                 (CASE WHEN OverallScore > Perc25Score THEN 1 ELSE 0 END) AS HigherThan25Perc,
#                 (CASE WHEN OverallScore > MedianScore THEN 1 ELSE 0 END) AS HigherThanMedian,
#                 (CASE WHEN OverallScore > Perc75Score THEN 1 ELSE 0 END) AS HigherThan75Perc,
#                 (CASE WHEN OverallScore = MinScore THEN 1 ELSE 0 END) AS PopulationMinimum
#             FROM PrepareData ORDER BY Date;
#         """.format(hourly_trends=_hourly_trends_table, population_scores=_population_aggregates)
#
#
# class EmptyCoinAggregates(TruncateTableQuery):
#     date_hour = luigi.DateHourParameter()
#     table = _coin_aggregates_table
#
#     def requires(self):
#         return InsertHourlyValues(**self.param_kwargs)
#
#
# class InsertCoinAggregates(DatabaseQuery):
#     date_hour = luigi.DateHourParameter()
#
#     def requires(self):
#         return EmptyCoinAggregates(**self.param_kwargs)
#
#     @property
#     def sql(self):
#         hour = self.date_hour.hour
#         current_date = self.date_hour.date()
#         yesterday = current_date - timedelta(days=1)
#         last_week = current_date - timedelta(days=7)
#         last_month = current_date - timedelta(days=30)
#
#         return """
#             WITH CurrentData AS (
#             SELECT
#                 *
#             FROM {values_table}
#             WHERE (DATEADD(HOUR, Hour, CAST(Date as datetime))) = (DATEADD(HOUR, {hour}, CAST('{current_date}' as datetime)))
#             ),
#             YesterdaysData AS (
#             SELECT
#                 CoinId,
#                 Price AS Price24HoursAgo
#             FROM {values_table}
#             WHERE (DATEADD(HOUR, Hour, CAST(Date as datetime))) = (DATEADD(HOUR, {hour}, CAST('{yesterday}' AS datetime)))
#             ),
#             LastWeeksData AS (
#             SELECT
#                 CoinId,
#                 Price AS Price1WeekAgo
#             FROM {values_table}
#             WHERE (DATEADD(HOUR, Hour, CAST(Date as datetime))) = (DATEADD(HOUR, {hour}, CAST('{last_week}' AS datetime)))
#             ),
#             LastMonthsData AS (
#             SELECT
#                 CoinId,
#                 Price AS Price1MonthAgo
#             FROM {values_table}
#             WHERE (DATEADD(HOUR, Hour, CAST(Date as datetime))) = (DATEADD(HOUR, {hour}, CAST('{last_month}' AS datetime)))
#             ),
#             DataYesterday AS (
#             SELECT
#                 CoinId,
#                 STDEV(Price) / (CASE WHEN AVG(Price) != 0 THEN AVG(Price) ELSE NULL END) AS YesterdayVolatility
#             FROM {values_table}
#             WHERE (DATEADD(HOUR, Hour, CAST(Date as datetime))) >= (DATEADD(HOUR, {hour}, CAST('{yesterday}' AS datetime)))
#             GROUP BY CoinId
#             ),
#             DataLastWeek AS (
#             SELECT
#                 CoinId,
#                 STDEV(Price) / (CASE WHEN AVG(Price) != 0 THEN AVG(Price) ELSE NULL END) AS LastWeekVolatility
#             FROM {values_table}
#             WHERE (DATEADD(HOUR, Hour, CAST(Date as datetime))) >= (DATEADD(HOUR, {hour}, CAST('{last_week}' AS datetime)))
#             GROUP BY CoinId
#             ),
#             DataLastMonth AS (
#             SELECT
#                 CoinId,
#                 STDEV(Price) / (CASE WHEN AVG(Price) != 0 THEN AVG(Price) ELSE NULL END) AS LastMonthVolatility
#             FROM {values_table}
#             WHERE (DATEADD(HOUR, Hour, CAST(Date as datetime))) >= (DATEADD(HOUR, {hour}, CAST('{last_month}' AS datetime)))
#             GROUP BY CoinId
#             ),
#             PreparedData AS (
#             SELECT CurrentData.CoinId,
#                     Symbol,
#                     Price,
#                     YesterdayVolatility,
#                     LastWeekVolatility,
#                     LastMonthVolatility,
#                     (Price / (CASE WHEN Price24HoursAgo != 0 THEN Price24HoursAgo ELSE NULL END) * 100 - 100) AS DayChange,
#                     (Price / (CASE WHEN Price1WeekAgo != 0 THEN Price1WeekAgo ELSE NULL END) * 100 - 100) AS WeekChange,
#                     (Price / (CASE WHEN Price1MonthAgo != 0 THEN Price1MonthAgo ELSE NULL END) * 100 - 100) AS MonthChange
#             FROM (
#                 CurrentData
#                     INNER JOIN YesterdaysData ON CurrentData.CoinId = YesterdaysData.CoinId
#                     INNER JOIN LastWeeksData ON CurrentData.CoinId = LastWeeksData.CoinId
#                     INNER JOIN LastMonthsData ON CurrentData.CoinId = LastMonthsData.CoinId
#                     INNER JOIN DataYesterday ON CurrentData.CoinId = DataYesterday.CoinId
#                     INNER JOIN DataLastWeek ON CurrentData.CoinId = DataLastWeek.CoinId
#                     INNER JOIN DataLastMonth ON CurrentData.CoinId = DataLastMonth.CoinId
#                 )
#             ),
#             Aggregates AS (
#             SELECT
#                 PreparedData.*,
#                 (CASE WHEN MIN(YesterdayVolatility) OVER (PARTITION BY 1) = YesterdayVolatility THEN 1 ELSE 0 END) AS Min24hVolatility,
#                 (CASE WHEN PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY YesterdayVolatility) OVER (PARTITION BY 1) > YesterdayVolatility THEN 1 ELSE 0 END) AS Small24hVolatility,
#                 (CASE WHEN PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY YesterdayVolatility) OVER (PARTITION BY 1) > YesterdayVolatility THEN 1 ELSE 0 END) AS Medium24hVolatility,
#                 (CASE WHEN PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY YesterdayVolatility) OVER (PARTITION BY 1) > YesterdayVolatility THEN 1 ELSE 0 END) AS High24hVolatility,
#                 (CASE WHEN MAX(YesterdayVolatility) OVER (PARTITION BY 1) = YesterdayVolatility THEN 1 ELSE 0 END) AS Max24hVolatility,
#
#                 (CASE WHEN MIN(DayChange) OVER (PARTITION BY 1) = DayChange THEN 1 ELSE 0 END) AS Min24hChange,
#                 (CASE WHEN PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY DayChange) OVER (PARTITION BY 1) > DayChange THEN 1 ELSE 0 END) AS Small24hChange,
#                 (CASE WHEN PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY DayChange) OVER (PARTITION BY 1) > DayChange THEN 1 ELSE 0 END) AS Medium24hChange,
#                 (CASE WHEN PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY DayChange) OVER (PARTITION BY 1) > DayChange THEN 1 ELSE 0 END) AS High24hChange,
#                 (CASE WHEN MAX(DayChange) OVER (PARTITION BY 1) = DayChange THEN 1 ELSE 0 END) AS Max24hChange,
#
#                 (CASE WHEN MIN(LastWeekVolatility) OVER (PARTITION BY 1) = LastWeekVolatility THEN 1 ELSE 0 END) AS MinWeekVolatility,
#                 (CASE WHEN PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY LastWeekVolatility) OVER (PARTITION BY 1) > LastWeekVolatility THEN 1 ELSE 0 END) AS SmallWeekVolatility,
#                 (CASE WHEN PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY LastWeekVolatility) OVER (PARTITION BY 1) > LastWeekVolatility THEN 1 ELSE 0 END) AS MediumWeekVolatility,
#                 (CASE WHEN PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY LastWeekVolatility) OVER (PARTITION BY 1) > LastWeekVolatility THEN 1 ELSE 0 END) AS HighWeekVolatility,
#                 (CASE WHEN MAX(YesterdayVolatility) OVER (PARTITION BY 1) = LastWeekVolatility THEN 1 ELSE 0 END) AS MaxWeekVolatility,
#
#                 (CASE WHEN MIN(WeekChange) OVER (PARTITION BY 1) = WeekChange THEN 1 ELSE 0 END) AS MinWeekChange,
#                 (CASE WHEN PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY WeekChange) OVER (PARTITION BY 1) > WeekChange THEN 1 ELSE 0 END) AS SmallWeekChange,
#                 (CASE WHEN PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY WeekChange) OVER (PARTITION BY 1) > WeekChange THEN 1 ELSE 0 END) AS MediumWeekChange,
#                 (CASE WHEN PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY WeekChange) OVER (PARTITION BY 1) > WeekChange THEN 1 ELSE 0 END) AS HighWeekChange,
#                 (CASE WHEN MAX(WeekChange) OVER (PARTITION BY 1) = WeekChange THEN 1 ELSE 0 END) AS MaxWeekChange,
#
#                 (CASE WHEN MIN(LastMonthVolatility) OVER (PARTITION BY 1) = LastMonthVolatility THEN 1 ELSE 0 END) AS MinMonthVolatility,
#                 (CASE WHEN PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY LastMonthVolatility) OVER (PARTITION BY 1) > LastWeekVolatility THEN 1 ELSE 0 END) AS SmallMonthVolatility,
#                 (CASE WHEN PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY LastMonthVolatility) OVER (PARTITION BY 1) > LastWeekVolatility THEN 1 ELSE 0 END) AS MediumMonthVolatility,
#                 (CASE WHEN PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY LastMonthVolatility) OVER (PARTITION BY 1) > LastWeekVolatility THEN 1 ELSE 0 END) AS HighMonthVolatility,
#                 (CASE WHEN MAX(LastMonthVolatility) OVER (PARTITION BY 1) = LastMonthVolatility THEN 1 ELSE 0 END) AS MaxMonthVolatility,
#
#                 (CASE WHEN MIN(MonthChange) OVER (PARTITION BY 1) = MonthChange THEN 1 ELSE 0 END) AS MinMonthChange,
#                 (CASE WHEN PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY MonthChange) OVER (PARTITION BY 1) > WeekChange THEN 1 ELSE 0 END) AS SmallMonthChange,
#                 (CASE WHEN PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY MonthChange) OVER (PARTITION BY 1) > WeekChange THEN 1 ELSE 0 END) AS MediumMonthChange,
#                 (CASE WHEN PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY MonthChange) OVER (PARTITION BY 1) > WeekChange THEN 1 ELSE 0 END) AS HighMonthChange,
#                 (CASE WHEN MAX(MonthChange) OVER (PARTITION BY 1) = MonthChange THEN 1 ELSE 0 END) AS MaxMonthChange
#             FROM PreparedData
#             )
#
#
#             INSERT INTO {coin_aggregates}
#             SELECT
#                 CoinId,
#                 Symbol,
#                 Price,
#                 DayChange,
#                 WeekChange,
#                 MonthChange,
#
#                 Min24hVolatility,
#                 Small24hVolatility,
#                 Medium24hVolatility,
#                 High24hVolatility,
#                 Max24hVolatility,
#
#                 Min24hChange,
#                 Small24hChange,
#                 Medium24hChange,
#                 High24hChange,
#                 Max24hChange,
#
#                 MinWeekVolatility,
#                 SmallWeekVolatility,
#                 MediumWeekVolatility,
#                 HighWeekVolatility,
#                 MaxWeekVolatility,
#
#                 MinWeekChange,
#                 SmallWeekChange,
#                 MediumWeekChange,
#                 HighWeekChange,
#                 MaxWeekChange,
#
#                 MinMonthChange,
#                 SmallMonthChange,
#                 MediumMonthChange,
#                 HighMonthChange,
#                 MaxMonthChange,
#
#                 MinMonthVolatility,
#                 SmallMonthVolatility,
#                 MediumMonthVolatility,
#                 HighMonthVolatility,
#                 MaxMonthVolatility
#             FROM Aggregates;
#         """.format(coin_aggregates=_coin_aggregates_table,
#                    values_table=_values_table,
#                    hour=hour,
#                    current_date=current_date,
#                    yesterday=yesterday,
#                    last_week=last_week,
#                    last_month=last_month)
